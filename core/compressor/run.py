#!/usr/bin/env python3
"""
QuantLab Parquet Compaction Runner
Implements state-based catch-up logic
"""

import os
import sys
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
from compact import CompactionJob, get_today_date, format_bytes, logger


def main():
    # Load environment
    env_path = Path(__file__).parent.parent / '.env'
    load_dotenv(env_path)
    
    # Get configuration
    s3_endpoint = os.getenv('S3_ENDPOINT')
    s3_access_key = os.getenv('S3_ACCESS_KEY')
    s3_secret_key = os.getenv('S3_SECRET_KEY')
    raw_bucket = os.getenv('S3_BUCKET', 'quantlab-raw')
    compact_bucket = os.getenv('S3_COMPACT_BUCKET', 'quantlab-compact')
    
    if not all([s3_endpoint, s3_access_key, s3_secret_key]):
        logger.error("Missing S3 configuration in .env")
        sys.exit(1)
        
    job = CompactionJob(
        s3_endpoint=s3_endpoint,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
        raw_bucket=raw_bucket,
        compact_bucket=compact_bucket
    )
    
    # 1. Read current state
    last_date = job.state_manager.get_last_compacted_date()
    today = get_today_date()
    
    logger.info("=" * 60)
    logger.info(f"QuantLab Compaction Catch-Up Job | Today: {today}")
    logger.info(f"Last Compacted Date: {last_date or 'None (Fresh Start)'}")
    logger.info("=" * 60)
    
    # 2. Discover available dates (O(1) Prefix-based)
    available_dates = sorted(list(job.discover_dates()))
    
    # 3. Calculate missing dates
    missing_dates = []
    if last_date is None:
        # FRESH START behavior: only process yesterday (if available in raw)
        from compact import get_yesterday_date
        yesterday = get_yesterday_date()
        if yesterday in available_dates:
            missing_dates = [yesterday]
            logger.info(f"Fresh Start detected: Only processing yesterday ({yesterday}) as per operational policy.")
    else:
        for d in available_dates:
            if d >= today:
                continue # Partial day protection
            if d > last_date:
                missing_dates.append(d)
            
    if not missing_dates:
        logger.info("No missing days to compact. Catch-up complete.")
        sys.exit(0)
        
    logger.info(f"Catch-up required for {len(missing_dates)} days: {missing_dates}")
    
    # 4. Process each missing day IN ORDER
    for target_date in missing_dates:
        logger.info(f"\n>>> PROCESSING DATE: {target_date}")
        partitions = job.discover_partitions_for_date(target_date)
        
        if not partitions:
            logger.warning(f"No partitions found for {target_date}, skipping state update.")
            continue
            
        logger.info(f"Found {len(partitions)} partitions for {target_date}")
        
        day_success = True
        total_input = 0
        total_output = 0
        
        for partition in partitions:
            result = job.compact_date_partition(
                exchange=partition['exchange'],
                stream=partition['stream'],
                symbol=partition['symbol'],
                date=partition['date']
            )
            
            if result['status'] == 'failed':
                day_success = False
                logger.error(f"Day failed at partition {partition['symbol']}/{partition['stream']}")
                break # Stop processing this day on failure
                
            total_input += result['total_size_bytes']
            total_output += result['output_size_bytes']
            
        if day_success:
            # Update state ONLY after full day success
            job.state_manager.update_last_compacted_date(target_date)
            logger.info(f"--- SUCCESS: {target_date} fully compacted ---")
            if total_input > 0:
                comp = (1 - total_output / total_input) * 100
                logger.info(f"Day Stats: {format_bytes(total_input)} -> {format_bytes(total_output)} ({comp:.1f}% compression)")
        else:
            logger.error(f"!!! FAILED: {target_date} incomplete, stopping catch-up loop.")
            sys.exit(1)
            
    logger.info("\n" + "=" * 60)
    logger.info("CATCH-UP COMPLETE")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
