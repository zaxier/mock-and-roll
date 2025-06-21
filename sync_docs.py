#!/usr/bin/env python3
"""
Document Synchronization Script for CLAUDE.md and .goosehints

This script helps maintain synchronization between CLAUDE.md and .goosehints files
by detecting differences and providing options to resolve them.

Usage:
    python sync_docs.py                    # Interactive mode
    python sync_docs.py --dry-run         # Preview only
    python sync_docs.py --auto-claude     # Auto-use CLAUDE.md as source
    python sync_docs.py --auto-goose      # Auto-use .goosehints as source
    python sync_docs.py --help           # Show this help
"""

import argparse
import difflib
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple


class DocumentSyncer:
    """Handles synchronization between CLAUDE.md and .goosehints files."""
    
    def __init__(self, repo_root: Path = None):
        self.repo_root = repo_root or Path.cwd()
        self.claude_file = self.repo_root / "CLAUDE.md"
        self.goose_file = self.repo_root / ".goosehints"
        self.backup_dir = self.repo_root / ".sync_backups"
        
    def check_file_status(self) -> Tuple[bool, bool]:
        """Check which files exist."""
        return self.claude_file.exists(), self.goose_file.exists()
    
    def read_file_content(self, file_path: Path) -> str:
        """Read file content safely."""
        try:
            return file_path.read_text(encoding='utf-8')
        except Exception as e:
            print(f"❌ Error reading {file_path.name}: {e}")
            sys.exit(1)
    
    def create_backup(self, file_path: Path) -> Path:
        """Create a timestamped backup of the file."""
        if not file_path.exists():
            return None
            
        self.backup_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{file_path.name}.backup.{timestamp}"
        backup_path = self.backup_dir / backup_name
        
        try:
            shutil.copy2(file_path, backup_path)
            print(f"📁 Created backup: {backup_path}")
            return backup_path
        except Exception as e:
            print(f"⚠️  Failed to create backup: {e}")
            return None
    
    def generate_diff(self, content1: str, content2: str, name1: str, name2: str) -> str:
        """Generate a unified diff between two file contents."""
        diff_lines = list(difflib.unified_diff(
            content1.splitlines(keepends=True),
            content2.splitlines(keepends=True),
            fromfile=name1,
            tofile=name2,
            lineterm=''
        ))
        return ''.join(diff_lines)
    
    def display_diff(self, diff_content: str) -> None:
        """Display diff with basic formatting."""
        if not diff_content.strip():
            print("✅ Files are identical - no differences found!")
            return
            
        print("\n" + "="*60)
        print("📋 DIFFERENCES DETECTED")
        print("="*60)
        
        lines = diff_content.split('\n')
        for line in lines:
            if line.startswith('+++') or line.startswith('---'):
                print(f"\033[1m{line}\033[0m")  # Bold headers
            elif line.startswith('+'):
                print(f"\033[92m{line}\033[0m")  # Green additions
            elif line.startswith('-'):
                print(f"\033[91m{line}\033[0m")  # Red deletions
            elif line.startswith('@@'):
                print(f"\033[94m{line}\033[0m")  # Blue context markers
            else:
                print(line)
        
        print("="*60)
    
    def copy_file(self, source: Path, target: Path, create_backup: bool = True) -> bool:
        """Copy source file to target, with optional backup."""
        try:
            if create_backup and target.exists():
                self.create_backup(target)
            
            shutil.copy2(source, target)
            print(f"✅ Successfully copied {source.name} → {target.name}")
            return True
        except Exception as e:
            print(f"❌ Failed to copy file: {e}")
            return False
    
    def interactive_menu(self) -> str:
        """Display interactive menu and get user choice."""
        print("\n🤔 What would you like to do?")
        print("   1. Make .goosehints authoritative (copy .goosehints → CLAUDE.md)")
        print("   2. Make CLAUDE.md authoritative (copy CLAUDE.md → .goosehints)")
        print("   3. Exit without making changes (manual resolution)")
        
        while True:
            choice = input("\nEnter your choice (1/2/3): ").strip()
            if choice in ['1', '2', '3']:
                return choice
            print("❌ Invalid choice. Please enter 1, 2, or 3.")
    
    def handle_missing_files(self, claude_exists: bool, goose_exists: bool) -> bool:
        """Handle cases where one or both files are missing."""
        if not claude_exists and not goose_exists:
            print("❌ Neither CLAUDE.md nor .goosehints exists!")
            print("   Please ensure you're running this from the repository root.")
            return False
        
        if not claude_exists:
            print("⚠️  CLAUDE.md is missing!")
            if input("Create CLAUDE.md from .goosehints? (y/N): ").lower().startswith('y'):
                return self.copy_file(self.goose_file, self.claude_file, create_backup=False)
            return False
        
        if not goose_exists:
            print("📝 .goosehints doesn't exist yet.")
            if input("Create .goosehints from CLAUDE.md? (y/N): ").lower().startswith('y'):
                return self.copy_file(self.claude_file, self.goose_file, create_backup=False)
            return False
        
        return True
    
    def sync_files(self, dry_run: bool = False, auto_mode: str = None) -> bool:
        """Main sync logic."""
        print("🔄 Document Synchronization Tool")
        print(f"📂 Repository: {self.repo_root}")
        print()
        
        claude_exists, goose_exists = self.check_file_status()
        
        # Handle missing files
        if not self.handle_missing_files(claude_exists, goose_exists):
            return False
        
        # Both files exist - check for differences
        claude_content = self.read_file_content(self.claude_file)
        goose_content = self.read_file_content(self.goose_file)
        
        # Generate and display diff
        diff_content = self.generate_diff(
            claude_content, goose_content, "CLAUDE.md", ".goosehints"
        )
        
        self.display_diff(diff_content)
        
        # If files are identical, nothing to do
        if not diff_content.strip():
            return True
        
        if dry_run:
            print("\n🔍 DRY RUN MODE - No changes will be made")
            return True
        
        # Determine action based on mode
        if auto_mode == 'claude':
            choice = '2'  # CLAUDE.md → .goosehints
            print(f"\n🤖 Auto mode: Using CLAUDE.md as authoritative source")
        elif auto_mode == 'goose':
            choice = '1'  # .goosehints → CLAUDE.md
            print(f"\n🤖 Auto mode: Using .goosehints as authoritative source")
        else:
            choice = self.interactive_menu()
        
        # Execute chosen action
        if choice == '1':
            print(f"\n🔄 Copying .goosehints → CLAUDE.md")
            return self.copy_file(self.goose_file, self.claude_file)
        elif choice == '2':
            print(f"\n🔄 Copying CLAUDE.md → .goosehints")
            return self.copy_file(self.claude_file, self.goose_file)
        else:
            print("\n👋 Exiting without changes. You can manually resolve the differences.")
            return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Synchronize CLAUDE.md and .goosehints files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        '--dry-run', 
        action='store_true', 
        help='Show differences without making changes'
    )
    parser.add_argument(
        '--auto-claude',
        action='store_true',
        help='Automatically use CLAUDE.md as authoritative source'
    )
    parser.add_argument(
        '--auto-goose',
        action='store_true',
        help='Automatically use .goosehints as authoritative source'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.auto_claude and args.auto_goose:
        print("❌ Cannot specify both --auto-claude and --auto-goose")
        sys.exit(1)
    
    # Determine auto mode
    auto_mode = None
    if args.auto_claude:
        auto_mode = 'claude'
    elif args.auto_goose:
        auto_mode = 'goose'
    
    # Run synchronization
    syncer = DocumentSyncer()
    try:
        success = syncer.sync_files(dry_run=args.dry_run, auto_mode=auto_mode)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n👋 Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()