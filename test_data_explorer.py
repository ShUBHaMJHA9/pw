#!/usr/bin/env python3
"""
Test Series Data Access Utility
Explore and query your downloaded test series data from the database and Internet Archive
"""

import os
import sys
import json
from urllib.parse import urlparse
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


class TestDataExplorer:
    """Utility to explore test series data stored in MySQL and Internet Archive"""
    
    def __init__(self, db_url=None):
        """Initialize database connection"""
        self.db_url = db_url or os.environ.get("PWDL_DB_URL")
        if not self.db_url:
            raise RuntimeError("PWDL_DB_URL not set")
        self.conn = None
        self._connect()
    
    def _connect(self):
        """Connect to MySQL database"""
        try:
            import pymysql
        except ImportError:
            raise RuntimeError("pymysql required: pip install pymysql")
        
        from urllib.parse import urlparse
        parsed = urlparse(self.db_url)
        
        self.conn = pymysql.connect(
            host=parsed.hostname,
            user=parsed.username,
            password=parsed.password,
            database=parsed.path.lstrip("/"),
            port=parsed.port or 3306,
            cursorclass=pymysql.cursors.DictCursor,
        )
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
    
    # ========================================================================
    # EXPLORATORY QUERIES
    # ========================================================================
    
    def list_batches(self):
        """List all batches with tests"""
        print("\n📚 BATCHES WITH TESTS:")
        print("=" * 80)
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT batch_id, COUNT(*) as test_count
                FROM tests
                GROUP BY batch_id
                ORDER BY batch_id
                """
            )
            results = cur.fetchall()
            for row in results:
                print(f"  • {row['batch_id']:<30} ({row['test_count']} tests)")
        return results
    
    def list_tests(self, batch_id):
        """List all tests in a batch"""
        print(f"\n📝 TESTS IN BATCH: {batch_id}")
        print("=" * 80)
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT test_id, test_name, test_type, 
                       (SELECT COUNT(*) FROM test_questions 
                        WHERE batch_id=%s AND test_id=tests.test_id) as question_count,
                       status, created_at
                FROM tests
                WHERE batch_id=%s
                ORDER BY created_at DESC
                """,
                (batch_id, batch_id)
            )
            results = cur.fetchall()
            for row in results:
                status_icon = "✓" if row['status'] == 'done' else "⏳" if row['status'] == 'pending' else "✗"
                print(f"  {status_icon} {row['test_id']:<30} {row['test_name']}")
                print(f"     Type: {row['test_type']}, Questions: {row['question_count']}, Status: {row['status']}")
        return results
    
    def get_test_summary(self, batch_id, test_id):
        """Get complete test summary with all metadata"""
        print(f"\n🎯 TEST SUMMARY: {batch_id} / {test_id}")
        print("=" * 80)
        
        with self.conn.cursor() as cur:
            # Get test metadata
            cur.execute(
                "SELECT * FROM tests WHERE batch_id=%s AND test_id=%s",
                (batch_id, test_id)
            )
            test = cur.fetchone()
            
            if not test:
                print(f"❌ Test not found!")
                return None
            
            print(f"Test Name: {test['test_name']}")
            print(f"Test Type: {test['test_type']}")
            print(f"Language: {test['language_code']}")
            print(f"Created: {test['created_at']}")
            print()
            
            # Get questions summary
            cur.execute(
                """
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN question_type='single' THEN 1 ELSE 0 END) as single,
                       SUM(CASE WHEN question_type='multiple' THEN 1 ELSE 0 END) as multiple,
                       SUM(positive_marks) as total_marks
                FROM test_questions
                WHERE batch_id=%s AND test_id=%s
                """,
                (batch_id, test_id)
            )
            q_summary = cur.fetchone()
            
            print(f"Questions: {q_summary['total']}")
            print(f"  • Single-choice: {q_summary['single']}")
            print(f"  • Multiple-choice: {q_summary['multiple']}")
            print(f"  • Total marks: {q_summary['total_marks']}")
            print()
            
            # Get asset summary
            cur.execute(
                """
                SELECT asset_kind, COUNT(*) as count,
                       SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) as uploaded,
                       SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed
                FROM test_assets
                WHERE batch_id=%s AND test_id=%s
                GROUP BY asset_kind
                """,
                (batch_id, test_id)
            )
            assets = cur.fetchall()
            
            print(f"Assets:")
            for asset in assets:
                status_bar = "█" * asset['uploaded'] + "░" * asset['failed']
                print(f"  • {asset['asset_kind']:<20} {asset['count']:>3} ({status_bar})  {asset['uploaded']} done, {asset['failed']} failed")
        
        return test
    
    def get_questions(self, batch_id, test_id, limit=None):
        """Get all questions with marks and metadata"""
        print(f"\n❓ QUESTIONS: {batch_id} / {test_id}")
        print("=" * 120)
        
        with self.conn.cursor() as cur:
            query = """
                SELECT question_number, question_id, question_type,
                       positive_marks, negative_marks,
                       difficulty_level, subject_id, topic_id
                FROM test_questions
                WHERE batch_id=%s AND test_id=%s
                ORDER BY question_number
            """
            params = [batch_id, test_id]
            if limit:
                query += " LIMIT %s"
                params.append(limit)
            
            cur.execute(query, params)
            results = cur.fetchall()
            
            print(f"{'Q':<3} {'Type':<12} {'+Marks':<8} {'-Marks':<8} {'Diff':<5} {'Subject':<20} {'Topic':<20}")
            print("-" * 120)
            
            for q in results:
                subject = q['subject_id'] or '-'
                topic = q['topic_id'] or '-'
                print(f"{q['question_number']:<3} {q['question_type']:<12} {str(q['positive_marks']):<8} {str(q['negative_marks']):<8} {str(q['difficulty_level']):<5} {subject:<20} {topic:<20}")
        
        return results
    
    def get_question_details(self, batch_id, test_id, question_id):
        """Get all details for a specific question"""
        print(f"\n🔍 QUESTION DETAILS: {question_id}")
        print("=" * 80)
        
        with self.conn.cursor() as cur:
            # Question metadata
            cur.execute(
                "SELECT * FROM test_questions WHERE batch_id=%s AND test_id=%s AND question_id=%s",
                (batch_id, test_id, question_id)
            )
            question = cur.fetchone()
            
            if not question:
                print(f"❌ Question not found!")
                return None
            
            print(f"Question ID: {question['question_id']}")
            print(f"Question #: {question['question_number']}")
            print(f"Type: {question['question_type']}")
            print(f"Marks: +{question['positive_marks']}/-{question['negative_marks']}")
            print(f"Difficulty: {question['difficulty_level']}")
            print(f"Subject: {question['subject_id']}")
            print(f"Topic: {question['topic_id']}")
            print()
            
            # Answer options
            cur.execute(
                "SELECT option_id, option_text FROM test_options WHERE batch_id=%s AND test_id=%s AND question_id=%s",
                (batch_id, test_id, question_id)
            )
            options = cur.fetchall()
            
            if options:
                print(f"Options ({len(options)}):")
                print("-" * 80)
                for opt in options:
                    text_preview = opt['option_text'][:60] if opt['option_text'] else '(empty)'
                    if len(opt['option_text'] or '') > 60:
                        text_preview += "..."
                    print(f"  {opt['option_id']}: {text_preview}")
                print()
            
            # Assets (images and videos)
            cur.execute(
                """
                SELECT asset_kind, storage_provider, storage_url, youtube_id, status
                FROM test_assets
                WHERE batch_id=%s AND test_id=%s AND question_id=%s
                ORDER BY asset_kind
                """,
                (batch_id, test_id, question_id)
            )
            assets = cur.fetchall()
            
            if assets:
                print(f"Assets ({len(assets)}):")
                print("-" * 80)
                for asset in assets:
                    print(f"  [{asset['asset_kind']:<20}] {asset['status']}")
                    if asset['storage_provider'] == 'youtube':
                        print(f"    YouTube: https://www.youtube.com/watch?v={asset['youtube_id']}")
                    else:
                        print(f"    URL: {asset['storage_url'][:70]}...")
                print()
        
        return question
    
    def get_asset_stats(self, batch_id, test_id):
        """Get asset upload statistics"""
        print(f"\n📊 ASSET STATISTICS: {batch_id} / {test_id}")
        print("=" * 80)
        
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT asset_kind, COUNT(*) as total,
                       SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) as uploaded,
                       SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) as pending,
                       SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed
                FROM test_assets
                WHERE batch_id=%s AND test_id=%s
                GROUP BY asset_kind
                ORDER BY asset_kind
                """,
                (batch_id, test_id)
            )
            results = cur.fetchall()
            
            total_all = 0
            uploaded_all = 0
            
            for row in results:
                total_all += row['total']
                uploaded_all += row['uploaded']
                pct = int(100 * row['uploaded'] / row['total']) if row['total'] > 0 else 0
                print(f"{row['asset_kind']:<20} {row['total']:>4} total  |  {row['uploaded']:>3} done  |  {row['pending']:>3} pending  |  {row['failed']:>3} failed  |  {pct:>3}%")
            
            if total_all > 0:
                pct_overall = int(100 * uploaded_all / total_all)
                print("-" * 80)
                print(f"{'TOTAL':<20} {total_all:>4} total  |  {uploaded_all:>3} done  |  {pct_overall:>3}%")
        
        return results
    
    def get_failed_assets(self, batch_id, test_id):
        """List failed asset uploads with error messages"""
        print(f"\n❌ FAILED ASSETS: {batch_id} / {test_id}")
        print("=" * 80)
        
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT question_id, asset_kind, source_key, source_url, error_text
                FROM test_assets
                WHERE batch_id=%s AND test_id=%s AND status='failed'
                ORDER BY question_id, asset_kind
                """,
                (batch_id, test_id)
            )
            results = cur.fetchall()
            
            if not results:
                print("✓ No failed assets!")
                return results
            
            for row in results:
                print(f"Question: {row['question_id']}")
                print(f"  Kind: {row['asset_kind']}")
                print(f"  Key: {row['source_key']}")
                print(f"  Source: {row['source_url'][:70]}...")
                print(f"  Error: {row['error_text']}")
                print()
        
        return results


def main():
    """CLI interface"""
    explorer = TestDataExplorer()
    
    try:
        # Interactive menu
        while True:
            print("\n" + "=" * 80)
            print("TEST DATA EXPLORER")
            print("=" * 80)
            print("\n1. List all batches")
            print("2. List tests in a batch")
            print("3. Get test summary")
            print("4. View questions")
            print("5. Get question details")
            print("6. Asset statistics")
            print("7. Failed assets")
            print("8. Exit\n")
            
            choice = input("Select option (1-8): ").strip()
            
            if choice == "1":
                explorer.list_batches()
            
            elif choice == "2":
                batch_id = input("Enter batch ID: ").strip()
                explorer.list_tests(batch_id)
            
            elif choice == "3":
                batch_id = input("Enter batch ID: ").strip()
                test_id = input("Enter test ID: ").strip()
                explorer.get_test_summary(batch_id, test_id)
            
            elif choice == "4":
                batch_id = input("Enter batch ID: ").strip()
                test_id = input("Enter test ID: ").strip()
                limit = input("Limit to N questions (press Enter for all): ").strip()
                limit = int(limit) if limit else None
                explorer.get_questions(batch_id, test_id, limit)
            
            elif choice == "5":
                batch_id = input("Enter batch ID: ").strip()
                test_id = input("Enter test ID: ").strip()
                question_id = input("Enter question ID: ").strip()
                explorer.get_question_details(batch_id, test_id, question_id)
            
            elif choice == "6":
                batch_id = input("Enter batch ID: ").strip()
                test_id = input("Enter test ID: ").strip()
                explorer.get_asset_stats(batch_id, test_id)
            
            elif choice == "7":
                batch_id = input("Enter batch ID: ").strip()
                test_id = input("Enter test ID: ").strip()
                explorer.get_failed_assets(batch_id, test_id)
            
            elif choice == "8":
                break
            
            else:
                print("Invalid choice!")
    
    finally:
        explorer.close()


if __name__ == "__main__":
    main()
