/**
 * RemoteJobRunner: Orchestrates ML job execution on remote GPU instance.
 * Handles: repo clone, deps install, job execution, artifact upload.
 */
import { Client } from 'ssh2';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import fs from 'fs';
import path from 'path';
import net from 'net';
import { exec } from 'child_process';
import { promisify } from 'util';
import { SCHEDULER_CONFIG } from '../scheduler/config.js';

const execPromise = promisify(exec);

export class RemoteJobRunner {
  #sshConfig;
  #s3Client;
  #s3Bucket;
  #vastClient;
  #instanceId;
  
  /**
   * @param {Object} sshConfig - SSH connection details (initially from vastClient)
   * @param {Object} vastClient - VastClient instance for port re-checks
   * @param {string} instanceId - The Vast instance ID
   */
  constructor(sshConfig, vastClient = null, instanceId = null) {
    this.#sshConfig = { ...sshConfig };
    this.#vastClient = vastClient;
    this.#instanceId = instanceId;
    
    // Initialize S3 client for artifact upload
    this.#s3Client = new S3Client({
      endpoint: SCHEDULER_CONFIG.s3.artifactEndpoint,
      region: process.env.S3_ARTIFACTS_REGION || process.env.S3_COMPACT_REGION || 'us-east-1',
      credentials: {
        accessKeyId: process.env.S3_COMPACT_ACCESS_KEY,
        secretAccessKey: process.env.S3_COMPACT_SECRET_KEY
      },
      forcePathStyle: true
    });
    this.#s3Bucket = SCHEDULER_CONFIG.s3.artifactBucket;
  }
  
  /**
   * Execute a training job on the remote GPU instance.
   * @param {Object} jobSpec - The job specification
   * @returns {Promise<Object>} Execution result with metrics
   */
  async executeJob(jobSpec) {
    console.log(`[RemoteJobRunner] Starting job ${jobSpec.jobId} on remote GPU...`);
    
    // Hotpatch block guard (v1.2) - FAIL FAST
    if (process.env.REMOTE_HOTPATCH === '1' || process.env.REMOTE_HOTPATCH === 'true') {
      throw new Error("REMOTE_HOTPATCH is disabled in v1.2. Please deploy your changes via git push.");
    }

    const startTime = Date.now();
    const logs = [];
    const conn = new Client();
    
    try {
      // 1. Wait for SSH readiness (Stage 1-3)
      await this.#waitForSshReady();
      
      // 2. Connect to instance
      await this.#connect(conn);
      logs.push({ step: 'connect', status: 'success' });
      
      // 2. Clone repository
      console.log(`[RemoteJobRunner] Cloning repository (branch: ${SCHEDULER_CONFIG.repo.branch})...`);
      await this.#exec(conn, `git clone --depth 1 --branch ${SCHEDULER_CONFIG.repo.branch} ${SCHEDULER_CONFIG.repo.url} /workspace/quantlab`);
      
      if (SCHEDULER_CONFIG.repo.commit) {
        console.log(`[RemoteJobRunner] Enforcing specific commit: ${SCHEDULER_CONFIG.repo.commit}...`);
        await this.#exec(conn, `cd /workspace/quantlab && git fetch --depth 1 origin ${SCHEDULER_CONFIG.repo.commit} && git checkout ${SCHEDULER_CONFIG.repo.commit}`);
      }
      logs.push({ step: 'clone', status: 'success', branch: SCHEDULER_CONFIG.repo.branch, commit: SCHEDULER_CONFIG.repo.commit });
      
      // 3. Install dependencies
      console.log('[RemoteJobRunner] Installing dependencies...');
      await this.#exec(conn, 'cd /workspace/quantlab/api && npm install --no-audit --no-fund');
      logs.push({ step: 'install', status: 'success' });
      
      // 4. Write job spec to file
      console.log('[RemoteJobRunner] Writing job spec...');
      const jobJson = JSON.stringify(jobSpec, null, 2);
      await this.#exec(conn, `cat > /workspace/job.json << 'EOF'\n${jobJson}\nEOF`);
      logs.push({ step: 'write_job', status: 'success' });

      
      // 5. Set environment variables for S3 access and Signal Rule
      const envVars = {
        S3_COMPACT_ENDPOINT: process.env.S3_COMPACT_ENDPOINT,
        S3_COMPACT_BUCKET: process.env.S3_COMPACT_BUCKET,
        S3_COMPACT_ACCESS_KEY: process.env.S3_COMPACT_ACCESS_KEY,
        S3_COMPACT_SECRET_KEY: process.env.S3_COMPACT_SECRET_KEY,
        S3_COMPACT_REGION: process.env.S3_COMPACT_REGION || 'us-east-1',
        // Fallback vars without _COMPACT
        S3_ENDPOINT: process.env.S3_COMPACT_ENDPOINT,
        S3_ACCESS_KEY: process.env.S3_COMPACT_ACCESS_KEY,
        S3_SECRET_KEY: process.env.S3_COMPACT_SECRET_KEY,
        S3_REGION: process.env.S3_COMPACT_REGION || 'us-east-1',
        QUANTLAB_DATA_DIR: `s3://${process.env.S3_COMPACT_BUCKET || 'quantlab-compact'}`,
        PSEUDO_PROBA: process.env.PSEUDO_PROBA || '0'
      };
      const envSetup = Object.entries(envVars)
        .map(([k, v]) => `export ${k}='${v}'`)
        .join(' && ');
      
      // 6. Execute training job
      console.log('[RemoteJobRunner] Executing training job...');
      const trainCmd = [
        'cd /workspace/quantlab/api',
        envSetup,
        'node ml/runtime/run-job.js /workspace/job.json'
      ].filter(Boolean).join(' && ');
      
      const trainOutput = await this.#exec(conn, trainCmd);
      logs.push({ step: 'train', status: 'success', output: trainOutput });
      
      // 7. Read metrics from remote
      console.log('[RemoteJobRunner] Reading metrics...');
      const metricsRaw = await this.#exec(conn, `cat /workspace/quantlab/api/${jobSpec.output.metricsPath}`);
      const metrics = JSON.parse(metricsRaw);
      logs.push({ step: 'read_metrics', status: 'success' });
      
      // 8. Upload artifacts to S3
      console.log('[RemoteJobRunner] Uploading artifacts to S3...');
      await this.#uploadArtifactsFromRemote(conn, jobSpec);
      logs.push({ step: 'upload', status: 'success' });
      
      const endTime = Date.now();
      const result = {
        jobId: jobSpec.jobId,
        success: true,
        metrics,
        runtimeMs: endTime - startTime,
        logs
      };
      
      console.log(`[RemoteJobRunner] Job ${jobSpec.jobId} completed in ${(endTime - startTime) / 1000}s`);
      return result;
      
    } catch (err) {
      console.error(`[RemoteJobRunner] Job ${jobSpec.jobId} failed:`, err.message);
      throw err;
    } finally {
      conn.end();
    }
  }
  
  /**
   * Public entry point for the SSH readiness gate.
   * Useful for external verification or pre-checking instances.
   */
  async waitForReady() {
    return await this.#waitForSshReady();
  }
  
  /**
   * SSH Zero-Patience Mode Constants
   */
  static SSH_HARD_TIMEOUT_MS = 45_000;   // Hard limit: 45 seconds
  static SSH_MAX_SLEEP_MS = 2000;         // Max cooldown between attempts
  static SSH_KEX_FATAL_PATTERNS = [
    'kex_exchange_identification',
    'Connection closed by remote host'
  ];

  /**
   * Wait for SSH to be fully ready using a 3-stage readiness gate.
   * SSH Zero-Patience Mode: Hard fail at 45s, immediate destroy on KEX fatal.
   * Includes port mapping re-checks if vastClient is available.
   */
  async #waitForSshReady() {
    console.log(`[SSH_READY] Starting readiness gate for ${this.#sshConfig.host}:${this.#sshConfig.port} (Zero-Patience Mode)...`);
    const startOverall = Date.now();
    const maxAttempts = 25;
    
    let attempt = 1;
    let delay = 1000; // Start at 1s, max 2s
    
    const keyPath = path.join(process.env.HOME, '.ssh', 'id_rsa');

    while (attempt <= maxAttempts) {
      const elapsedMs = Date.now() - startOverall;
      const elapsed = Math.floor(elapsedMs / 1000);

      // SSH Zero-Patience: Hard timeout at 45s
      if (elapsedMs > RemoteJobRunner.SSH_HARD_TIMEOUT_MS) {
        console.log(`[SSH_READY] HARD_FAIL reason=TIMEOUT elapsed=${elapsed}s`);
        await this.#destroyInstanceOnSshFail('TIMEOUT');
        throw new Error(`SSH_HARD_TIMEOUT: Instance failed SSH readiness after ${elapsed}s`);
      }

      // Step 0: Refresh port mapping from Vast if possible
      if (this.#vastClient && this.#instanceId) {
        try {
          const sshInfo = await this.#vastClient.getSshInfo(this.#instanceId);
          if (sshInfo.host !== this.#sshConfig.host || sshInfo.port !== this.#sshConfig.port) {
            console.log(`[SSH_READY] Port mapping changed: ${this.#sshConfig.host}:${this.#sshConfig.port} -> ${sshInfo.host}:${sshInfo.port}`);
            this.#sshConfig.host = sshInfo.host;
            this.#sshConfig.port = sshInfo.port;
          }
        } catch (err) {
          console.warn(`[SSH_READY] Warning: Failed to refresh port mapping: ${err.message}`);
        }
      }

      // Stage 1: TCP Port Probe (Fail-fast check)
      try {
        await this.#probeTcp();
      } catch (err) {
        console.log(`[SSH_READY] attempt=${attempt} elapsed=${elapsed}s stage=tcp err=${err.message}`);
        await new Promise(resolve => setTimeout(resolve, delay));
        delay = Math.min(delay * 1.5, RemoteJobRunner.SSH_MAX_SLEEP_MS);
        attempt++;
        continue;
      }

      // Stage 2 & 3: SSH and Node probes
      try {
        // Stage 2: SSH Noop Probe (with KEX fatal detection)
        await this.#probeSshZeroPatience(keyPath, elapsed);
        
        // Stage 3: Node.js Readiness Probe
        await this.#probeNode(keyPath);
        
        console.log(`[SSH_READY] SUCCESS: Ready confirmed in ${elapsed}s (attempt ${attempt})`);
        return;
        
      } catch (err) {
        // Check for KEX fatal errors - destroy immediately
        if (err.message.includes('SSH_KEX_FATAL')) {
          throw err; // Already destroyed, re-throw
        }
        
        const stage = err.message.includes('SSH_FAIL') ? 'ssh' : 'node';
        console.log(`[SSH_READY] attempt=${attempt} elapsed=${elapsed}s stage=${stage} err=${err.message}`);
        
        await new Promise(resolve => setTimeout(resolve, delay));
        delay = Math.min(delay * 1.5, RemoteJobRunner.SSH_MAX_SLEEP_MS);
        attempt++;
      }
    }
    
    // Max attempts reached - destroy and fail
    console.log(`[SSH_READY] HARD_FAIL reason=MAX_ATTEMPTS elapsed=${Math.floor((Date.now() - startOverall) / 1000)}s`);
    await this.#destroyInstanceOnSshFail('MAX_ATTEMPTS');
    throw new Error(`SSH_HARD_TIMEOUT: Max attempts (${maxAttempts}) reached`);
  }

  /**
   * SSH Probe with Zero-Patience KEX fatal detection.
   */
  async #probeSshZeroPatience(keyPath, elapsed) {
    const cmd = `ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5 -o BatchMode=yes -o IdentitiesOnly=yes -i ${keyPath} -p ${this.#sshConfig.port} root@${this.#sshConfig.host} "echo ok" 2>&1`;
    try {
      const { stdout, stderr } = await execPromise(cmd);
      const output = stdout + (stderr || '');
      
      // Check for KEX fatal patterns in output
      for (const pattern of RemoteJobRunner.SSH_KEX_FATAL_PATTERNS) {
        if (output.includes(pattern)) {
          console.log(`[SSH_READY] HARD_FAIL reason=KEX_FATAL elapsed=${elapsed}s pattern="${pattern}"`);
          await this.#destroyInstanceOnSshFail('KEX_FATAL');
          throw new Error(`SSH_KEX_FATAL: Detected "${pattern}" - instance is garbage`);
        }
      }
      
      if (!output.includes('ok')) {
        throw new Error(`Unexpected output: ${output.substring(0, 100)}`);
      }
    } catch (err) {
      // Check stderr for KEX fatal patterns
      const errMsg = err.message || '';
      const stderr = err.stderr || '';
      const combined = errMsg + stderr;
      
      for (const pattern of RemoteJobRunner.SSH_KEX_FATAL_PATTERNS) {
        if (combined.includes(pattern)) {
          console.log(`[SSH_READY] HARD_FAIL reason=KEX_FATAL elapsed=${elapsed}s pattern="${pattern}"`);
          await this.#destroyInstanceOnSshFail('KEX_FATAL');
          throw new Error(`SSH_KEX_FATAL: Detected "${pattern}" - instance is garbage`);
        }
      }
      
      throw new Error(`SSH_FAIL: ${err.message}`);
    }
  }

  /**
   * Destroy instance on SSH hard fail (Zero-Patience cleanup).
   */
  async #destroyInstanceOnSshFail(reason) {
    if (this.#vastClient && this.#instanceId) {
      console.log(`[VastClient] Destroying instance ${this.#instanceId} (ssh hard fail: ${reason})`);
      try {
        await this.#vastClient.destroyInstance(this.#instanceId);
      } catch (e) {
        console.warn(`[VastClient] Destroy failed: ${e.message}`);
      }
    }
  }

  /** Stage 1: TCP Probe */
  #probeTcp() {
    return new Promise((resolve, reject) => {
      const socket = net.connect({
        host: this.#sshConfig.host,
        port: this.#sshConfig.port,
        timeout: 3000
      });
      socket.on('connect', () => { socket.destroy(); resolve(); });
      socket.on('error', (err) => reject(new Error(`TCP_FAIL: ${err.message}`)));
      socket.on('timeout', () => { socket.destroy(); reject(new Error('TCP_FAIL: timeout')); });
    });
  }

  /** Stage 2: SSH Noop Probe */
  async #probeSsh(keyPath) {
    // We use child_process.exec to run system ssh command which is often more robust for initial probes
    // and easily supports the specific options Vast requires.
    const cmd = `ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5 -o BatchMode=yes -o IdentitiesOnly=yes -i ${keyPath} -p ${this.#sshConfig.port} root@${this.#sshConfig.host} "echo ok"`;
    try {
      const { stdout } = await execPromise(cmd);
      if (stdout.trim() !== 'ok') throw new Error(`Unexpected output: ${stdout}`);
    } catch (err) {
      throw new Error(`SSH_FAIL: ${err.message}`);
    }
  }

  /** Stage 3: Node Readiness Probe */
  async #probeNode(keyPath) {
    const cmd = `ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5 -o BatchMode=yes -o IdentitiesOnly=yes -i ${keyPath} -p ${this.#sshConfig.port} root@${this.#sshConfig.host} "node -v"`;
    try {
      const { stdout } = await execPromise(cmd);
      const version = stdout.trim();
      if (!version.startsWith('v')) throw new Error(`Invalid node version: ${version}`);
    } catch (err) {
      throw new Error(`NODE_FAIL: environment not fully initialized (missing node)`);
    }
  }
  
  /**
   * Connect to remote instance via SSH.
   */
  #connect(conn) {
    return new Promise((resolve, reject) => {
      conn.on('ready', () => resolve())
        .on('error', reject)
        .connect({
          host: this.#sshConfig.host,
          port: this.#sshConfig.port,
          username: this.#sshConfig.username,
          privateKey: this.#sshConfig.privateKey || fs.readFileSync(
            path.join(process.env.HOME, '.ssh', 'id_rsa')
          )
        });
    });
  }

  /**
   * Execute command on remote instance.
   */
  #exec(conn, command) {
    return new Promise((resolve, reject) => {
      conn.exec(command, { pty: false }, (err, stream) => {
        if (err) return reject(err);
        
        let stdout = '';
        let stderr = '';
        
        stream.on('data', data => { stdout += data; });
        stream.stderr.on('data', data => { stderr += data; });
        stream.on('close', (code) => {
          if (code === 0) {
            resolve(stdout.trim());
          } else {
            reject(new Error(`Command failed (exit ${code}): ${stderr || stdout}`));
          }
        });
      });
    });
  }
  
  /**
   * Upload job artifacts from remote to S3.
   */
  async #uploadArtifactsFromRemote(conn, jobSpec) {
    const s3Prefix = `${SCHEDULER_CONFIG.s3.artifactPrefix}/${jobSpec.id || jobSpec.jobId}`;
    const bucket = SCHEDULER_CONFIG.s3.artifactBucket;

    const files = [
      { name: 'model.bin', binary: true },
      { name: 'metrics.json', binary: false },
      { name: 'runtime.json', binary: false },
      { name: 'job.json', binary: false }
    ];

    for (const file of files) {
      try {
        let remotePath = file.name === 'job.json' 
          ? '/workspace/job.json'
          : `/workspace/quantlab/api/ml/artifacts/jobs/${jobSpec.id || jobSpec.jobId}/${file.name}`;

        console.log(`[RemoteJobRunner] Uploading ${file.name} to ${bucket}/${s3Prefix}/${file.name}...`);
        
        let body;
        if (file.binary) {
          const b64 = await this.#exec(conn, `cat ${remotePath} | base64 -w 0`);
          if (!b64) continue;
          body = Buffer.from(b64, 'base64');
        } else {
          body = await this.#exec(conn, `cat ${remotePath}`);
          if (!body) continue;
        }

        await this.#s3Client.send(new PutObjectCommand({
          Bucket: bucket,
          Key: `${s3Prefix}/${file.name}`,
          Body: body,
          ContentType: file.binary ? 'application/octet-stream' : 'application/json'
        }));
        
        console.log(`[RemoteJobRunner] Uploaded ${file.name} successfully`);
      } catch (err) {
        console.warn(`[RemoteJobRunner] Warning: Could not upload ${file.name}:`, err.message);
      }
    }
  }
  
  /**
   * Test SSH connection without executing job.
   */
  async testConnection() {
    const conn = new Client();
    try {
      await this.#connect(conn);
      const hostname = await this.#exec(conn, 'hostname');
      console.log(`[RemoteJobRunner] Connected to: ${hostname}`);
      return { success: true, hostname };
    } finally {
      conn.end();
    }
  }
  /**
   * Upload a local file to the remote instance.
   */
  async #uploadFileToRemote(conn, localPath, remotePath) {
    const content = fs.readFileSync(localPath);
    const b64 = content.toString('base64');
    // Upload via base64 decoding
    await this.#exec(conn, `echo "${b64}" | base64 -d > ${remotePath}`);
  }
}
