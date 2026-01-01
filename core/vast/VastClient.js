/**
 * VastClient: REST API client for Vast.ai GPU orchestration.
 * No CLI dependency - pure HTTP requests.
 */
import https from 'https';

export class VastClient {
  #apiKey;
  #baseUrl = 'https://console.vast.ai';
  
  constructor(apiKey) {
    if (!apiKey) {
      throw new Error('VastClient: API key is required');
    }
    this.#apiKey = apiKey;
  }
  
  /**
   * Make authenticated API request.
   */
  async #request(method, path, body = null) {
    return new Promise((resolve, reject) => {
      const url = new URL(path, this.#baseUrl);
      
      const options = {
        method,
        hostname: url.hostname,
        path: url.pathname + url.search,
        headers: {
          'Authorization': `Bearer ${this.#apiKey}`,
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        }
      };
      
      const req = https.request(options, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
          try {
            const parsed = data ? JSON.parse(data) : {};
            if (res.statusCode >= 200 && res.statusCode < 300) {
              resolve(parsed);
            } else {
              reject(new Error(`API Error ${res.statusCode}: ${JSON.stringify(parsed)}`));
            }
          } catch (e) {
            reject(new Error(`Failed to parse response: ${data}`));
          }
        });
      });
      
      req.on('error', reject);
      req.setTimeout(30000, () => {
        req.destroy();
        reject(new Error('Request timeout'));
      });
      
      if (body) {
        req.write(JSON.stringify(body));
      }
      req.end();
    });
  }
  
  /**
   * Search for available GPU offers matching requirements.
   * @param {Object} spec - Search criteria
   * @param {number} spec.minGpuMemory - Minimum GPU memory in GB
   * @param {number} spec.maxHourlyCost - Maximum hourly cost in USD
   * @param {number} spec.minDiskSpace - Minimum disk space in GB
   * @param {string[]} [spec.preferredTypes] - Preferred GPU types
   * @returns {Promise<Array>} List of matching offers sorted by price
   */
  async searchOffers(spec) {
    console.log('[VastClient] Searching for GPU offers (verified only)...');
    
    // Query parameters for offer search
    const query = new URLSearchParams({
      verified: 'true',
      external: 'false',
      rentable: 'true',
      gpu_ram: String(spec.minGpuMemory || 16),
      disk_space: String(spec.minDiskSpace || 20),
      order: 'dph_total',  // Sort by price
      type: 'on-demand'
    });
    
    const response = await this.#request('GET', `/api/v0/bundles/?${query}`);
    
    let offers = response.offers || [];
    
    // STRICT: Client-side verified filter (Vast API sometimes ignores this)
    offers = offers.filter(o => o.verification === 'verified');
    console.log(`[VastClient] After verified filter: ${offers.length} offers`);
    
    // STRICT: Only hosts with reliability score >= 0.9
    offers = offers.filter(o => (o.reliability2 || 0) >= 0.9);
    console.log(`[VastClient] After reliability filter (>=0.9): ${offers.length} offers`);
    
    // Filter by max cost
    if (spec.maxHourlyCost) {
      offers = offers.filter(o => o.dph_total <= spec.maxHourlyCost);
    }
    
    // Prefer specific GPU types if specified
    if (spec.preferredTypes && spec.preferredTypes.length > 0) {
      const preferredOffers = offers.filter(o => 
        spec.preferredTypes.some(t => o.gpu_name?.includes(t))
      );
      if (preferredOffers.length > 0) {
        offers = preferredOffers;
      }
    }
    
    console.log(`[VastClient] Found ${offers.length} matching offers`);
    return offers;
  }
  
  /**
   * Create a new GPU instance from an offer.
   * @param {string} offerId - The offer ID to accept
   * @param {Object} config - Instance configuration
   * @param {string} config.image - Docker image to use
   * @param {string} [config.onstart] - Startup script
   * @returns {Promise<Object>} Created instance details
   */
  async createInstance(offerId, config) {
    console.log(`[VastClient] Creating instance from offer ${offerId}...`);
    
    const body = {
      client_id: 'me',
      image: config.image || 'nvidia/cuda:12.0-devel-ubuntu22.04',
      disk: config.diskSpace || 20,
      onstart: config.onstart || '',
      label: config.label || 'quantlab-ml',
      env: config.env || {}
    };
    
    const response = await this.#request('PUT', `/api/v0/asks/${offerId}/`, body);
    
    const instanceId = response.new_contract;
    console.log(`[VastClient] Instance created: ${instanceId}`);
    
    return {
      instanceId,
      offerId,
      ...response
    };
  }
  
  /**
   * Get instance status.
   * @param {string} instanceId
   * @returns {Promise<Object>} Instance details
   */
  async getInstanceStatus(instanceId) {
    const response = await this.#request('GET', `/api/v0/instances/${instanceId}/`);
    // Single instance endpoint returns { instances: { ...data } }
    return response.instances || response;
  }
  
  /**
   * Wait until instance is ready (running state).
   * @param {string} instanceId
   * @param {number} [timeoutMs=300000] - Max wait time (default 5 min)
   * @returns {Promise<Object>} Instance details when ready
   */
  async waitUntilReady(instanceId, timeoutMs = 600000) {
    console.log(`[VastClient] Waiting for instance ${instanceId} to be ready...`);
    
    const startTime = Date.now();
    let delay = 5000; // Start with 5s, exponential backoff
    
    while (Date.now() - startTime < timeoutMs) {
      const status = await this.getInstanceStatus(instanceId);
      
      console.log(`[VastClient] Instance status: ${status.actual_status}`);
      
      if (status.actual_status === 'running') {
        console.log(`[VastClient] Instance ${instanceId} is ready!`);
        return status;
      }
      
      if (status.actual_status === 'error' || status.actual_status === 'exited' || 
          (status.status_msg && status.status_msg.includes('Error'))) {
        throw new Error(`Instance ${instanceId} failed: ${status.status_msg || 'Unknown error'}`);
      }
      
      // Wait with exponential backoff (max 30s)
      await this.#sleep(Math.min(delay, 30000));
      delay = Math.floor(delay * 1.5);
    }
    
    throw new Error(`Timeout waiting for instance ${instanceId} to be ready`);
  }
  
  /**
   * Destroy an instance. ALWAYS call this on completion or error.
   * @param {string} instanceId
   * @returns {Promise<void>}
   */
  async destroyInstance(instanceId) {
    console.log(`[VastClient] Destroying instance ${instanceId}...`);
    
    try {
      await this.#request('DELETE', `/api/v0/instances/${instanceId}/`);
      console.log(`[VastClient] Instance ${instanceId} destroyed successfully`);
    } catch (err) {
      console.error(`[VastClient] Warning: Failed to destroy instance ${instanceId}:`, err.message);
      // Log but don't throw - best effort cleanup
    }
  }
  
  /**
   * Get SSH connection details for an instance.
   * @param {string} instanceId
   * @returns {Promise<Object>} SSH connection info
   */
  async getSshInfo(instanceId) {
    const status = await this.getInstanceStatus(instanceId);
    return {
      host: status.ssh_host,
      port: status.ssh_port,
      username: 'root'  // Vast.ai default
    };
  }
  
  /**
   * List all current instances.
   * @returns {Promise<Array>} List of instances
   */
  async listInstances() {
    const response = await this.#request('GET', '/api/v0/instances/');
    return response.instances || [];
  }
  
  /**
   * Get account balance and usage info.
   * @returns {Promise<Object>}
   */
  async getAccountInfo() {
    const response = await this.#request('GET', '/api/v0/users/current/');
    return response;
  }
  
  #sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

/**
 * Create VastClient from environment.
 */
export function createVastClient() {
  const apiKey = process.env.VAST_API_KEY;
  if (!apiKey) {
    throw new Error('VAST_API_KEY environment variable is required');
  }
  return new VastClient(apiKey);
}
