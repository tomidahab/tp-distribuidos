#!/usr/bin/env python3
"""
Chaos Monkey for tp-distribuidos
Randomly kills worker containers to test fault tolerance.
Can kill multiple containers simultaneously for more chaotic scenarios.
Excludes: client, gateway, and rabbitmq containers.
"""

import subprocess
import time
import random
import signal
import sys
import argparse
from typing import List, Set

class ChaosMonkey:
    def __init__(self, min_interval: int = 10, max_interval: int = 60, kill_count: int = 1, dry_run: bool = False):
        self.min_interval = min_interval
        self.max_interval = max_interval
        self.kill_count_per_event = kill_count
        self.dry_run = dry_run
        self.running = True
        
        # Containers to NEVER kill (protected)
        self.protected_patterns = {
            'client_1', 'client_2', 'gateway', 'rabbitmq_server'
        }
        
        # Containers we CAN kill (workers)
        self.target_patterns = {
            'filter_by_amount_worker_',
            'filter_by_hour_worker_',
            'categorizer_q2_worker_',
            # 'categorizer_q3_worker_',
            # 'categorizer_q4_worker_',
            # 'birthday_dictionary_worker_'
        }
        
        # Track kills for statistics
        self.kill_count = 0
        self.kills_by_type = {}
        
        print(f"🐒 Chaos Monkey initialized")
        print(f"   Kill interval: {min_interval}-{max_interval} seconds")
        print(f"   Containers per event: {kill_count}")
        print(f"   Dry run mode: {'ON' if dry_run else 'OFF'}")
        print(f"   Protected: {', '.join(self.protected_patterns)}")
        print()

    def signal_handler(self, signum, frame):
        """Handle Ctrl+C gracefully"""
        print(f"\n🛑 Chaos Monkey stopping... (killed {self.kill_count} containers)")
        if self.kills_by_type:
            print("📊 Kill statistics:")
            for worker_type, count in sorted(self.kills_by_type.items()):
                print(f"   {worker_type}: {count} kills")
        self.running = False

    def get_running_containers(self) -> List[str]:
        """Get list of all running containers"""
        try:
            result = subprocess.run(
                ['docker', 'ps', '--format', '{{.Names}}'],
                capture_output=True, text=True, check=True
            )
            return [name.strip() for name in result.stdout.split('\n') if name.strip()]
        except subprocess.CalledProcessError as e:
            print(f"❌ Error getting container list: {e}")
            return []

    def is_killable_container(self, container_name: str) -> bool:
        """Check if a container can be killed"""
        # Never kill protected containers
        if container_name in self.protected_patterns:
            return False
        
        # Only kill containers that match our target patterns
        return any(pattern in container_name for pattern in self.target_patterns)

    def get_killable_containers(self) -> List[str]:
        """Get list of containers that can be killed"""
        all_containers = self.get_running_containers()
        killable = [c for c in all_containers if self.is_killable_container(c)]
        return killable

    def kill_random_containers(self) -> int:
        """Kill random killable containers. Returns number of containers actually killed."""
        killable_containers = self.get_killable_containers()
        
        if not killable_containers:
            print("⚠️  No killable containers found!")
            return 0
        
        # Determine how many to kill (up to available containers)
        targets_to_kill = min(self.kill_count_per_event, len(killable_containers))
        
        # Select random victims (without replacement)
        victims = random.sample(killable_containers, targets_to_kill)
        
        successful_kills = 0
        
        if self.dry_run:
            print(f"🎭 DRY RUN: Would kill {targets_to_kill} containers:")
            for victim in victims:
                worker_type = self._extract_worker_type(victim)
                print(f"   💀 {victim} ({worker_type})")
            return targets_to_kill
        
        print(f"💥 Chaos event: Killing {targets_to_kill} containers...")
        
        for victim in victims:
            worker_type = self._extract_worker_type(victim)
            
            try:
                # Kill the container
                subprocess.run(['docker', 'kill', victim], check=True, capture_output=True)
                
                self.kill_count += 1
                self.kills_by_type[worker_type] = self.kills_by_type.get(worker_type, 0) + 1
                successful_kills += 1
                
                print(f"   💀 Killed {victim} ({worker_type})")
                
            except subprocess.CalledProcessError as e:
                print(f"   ❌ Failed to kill {victim}: {e}")
        
        print(f"✅ Chaos event complete: {successful_kills}/{targets_to_kill} kills successful (Total: {self.kill_count})")
        return successful_kills

    def _extract_worker_type(self, container_name: str) -> str:
        """Extract worker type from container name for statistics"""
        for pattern in self.target_patterns:
            if pattern in container_name:
                return pattern.rstrip('_')
        return 'unknown'

    def show_status(self):
        """Show current system status"""
        all_containers = self.get_running_containers()
        killable = self.get_killable_containers()
        protected = [c for c in all_containers if c in self.protected_patterns]
        
        print(f"\n📊 System Status:")
        print(f"   Total containers: {len(all_containers)}")
        print(f"   Killable workers: {len(killable)}")
        print(f"   Protected: {len(protected)}")
        print(f"   Killable: {', '.join(killable) if killable else 'None'}")
        print(f"   Protected: {', '.join(protected) if protected else 'None'}")

    def run(self):
        """Main chaos monkey loop"""
        # Set up signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        print(f"🚀 Starting Chaos Monkey... Press Ctrl+C to stop")
        
        # Show initial status
        self.show_status()
        print()
        
        while self.running:
            try:
                # Wait random interval
                wait_time = random.randint(self.min_interval, self.max_interval)
                print(f"⏰ Waiting {wait_time} seconds until next chaos event...")
                
                for i in range(wait_time):
                    if not self.running:
                        break
                    time.sleep(1)
                
                if not self.running:
                    break
                
                # Execute chaos!
                self.kill_random_containers()
                
            except KeyboardInterrupt:
                self.signal_handler(None, None)
                break
            except Exception as e:
                print(f"❌ Unexpected error: {e}")
                time.sleep(5)

def main():
    parser = argparse.ArgumentParser(description='Chaos Monkey for tp-distribuidos')
    parser.add_argument('--min-interval', type=int, default=10, 
                       help='Minimum seconds between kills (default: 10)')
    parser.add_argument('--max-interval', type=int, default=60,
                       help='Maximum seconds between kills (default: 60)')
    parser.add_argument('--kill-count', type=int, default=1,
                       help='Number of containers to kill per chaos event (default: 1)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be killed without actually killing')
    parser.add_argument('--status', action='store_true',
                       help='Show current system status and exit')
    
    args = parser.parse_args()
    
    # Validate intervals
    if args.min_interval < 1:
        print("❌ min-interval must be at least 1 second")
        sys.exit(1)
    if args.max_interval < args.min_interval:
        print("❌ max-interval must be >= min-interval")
        sys.exit(1)
    if args.kill_count < 1:
        print("❌ kill-count must be at least 1")
        sys.exit(1)
    
    chaos_monkey = ChaosMonkey(
        min_interval=args.min_interval,
        max_interval=args.max_interval,
        kill_count=args.kill_count,
        dry_run=args.dry_run
    )
    
    if args.status:
        chaos_monkey.show_status()
        return
    
    try:
        chaos_monkey.run()
    except KeyboardInterrupt:
        print("\n👋 Chaos Monkey stopped by user")
    except Exception as e:
        print(f"💥 Chaos Monkey crashed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()