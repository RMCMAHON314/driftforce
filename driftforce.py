#!/usr/bin/env python3
"""DriftForce - Dead Simple Snowflake Schema Drift Detection"""
import os
import sys
import json
import argparse
import snowflake.connector
from datetime import datetime

print("DriftForce v1.0 - Schema Drift Detection")

class DriftForce:
    def __init__(self):
        # Get credentials from environment
        self.user = os.getenv('SNOWFLAKE_USER')
        self.password = os.getenv('SNOWFLAKE_PASSWORD')
        self.account = os.getenv('SNOWFLAKE_ACCOUNT')
        
        # Help users if not configured
        if not all([self.user, self.password, self.account]):
            print("\n⚠️  Setup Required (30 seconds):\n")
            print("1. Look at your Snowflake URL: https://[ACCOUNT].snowflakecomputing.com")
            print("2. Set these environment variables:\n")
            print("   export SNOWFLAKE_USER='your_username'")
            print("   export SNOWFLAKE_PASSWORD='your_password'")
            print("   export SNOWFLAKE_ACCOUNT='ABC12345.us-east-1'  # From URL\n")
            print("Example:")
            print("   export SNOWFLAKE_USER='john_doe'")
            print("   export SNOWFLAKE_PASSWORD='SecurePass123!'")
            print("   export SNOWFLAKE_ACCOUNT='SQC50998.us-east-1'\n")
            sys.exit(1)
    
    def connect(self, database, schema):
        """Connect to Snowflake"""
        try:
            return snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
                database=database,
                schema=schema,
                role=os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN')
            )
        except Exception as e:
            if '404' in str(e):
                print(f"\n❌ Connection failed: Account '{self.account}' not found")
                print("   Try different format: 'ABC12345' or 'ABC12345.region' or 'ORG-ABC12345'")
            elif 'Incorrect username or password' in str(e):
                print(f"\n❌ Login failed for user '{self.user}'")
                print("   Check your username and password")
            else:
                print(f"\n❌ Error: {e}")
            sys.exit(1)
    
    def snapshot(self, database, schema):
        """Take schema snapshot"""
        conn = self.connect(database, schema)
        cursor = conn.cursor()
        
        print(f"📸 Scanning {database}.{schema}...")
        
        # Get all tables and columns in one query for speed
        cursor.execute(f"""
            SELECT 
                c.TABLE_NAME,
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.IS_NULLABLE,
                c.ORDINAL_POSITION
            FROM {database}.INFORMATION_SCHEMA.COLUMNS c
            WHERE c.TABLE_SCHEMA = '{schema}'
            ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION
        """)
        
        tables = {}
        for row in cursor.fetchall():
            table_name = row[0]
            if table_name not in tables:
                tables[table_name] = {'columns': []}
            tables[table_name]['columns'].append({
                'name': row[1],
                'type': row[2],
                'nullable': row[3],
                'position': row[4]
            })
        
        cursor.close()
        conn.close()
        
        print(f"✅ Found {len(tables)} tables")
        
        return {
            'database': database,
            'schema': schema,
            'tables': tables,
            'timestamp': datetime.now().isoformat()
        }
    
    def detect_drifts(self, baseline, current):
        """Find differences between snapshots"""
        drifts = []
        
        old_tables = baseline['tables']
        new_tables = current['tables']
        
        # Check tables
        for table in set(old_tables) | set(new_tables):
            if table not in old_tables:
                drifts.append(f"➕ Table added: {table}")
            elif table not in new_tables:
                drifts.append(f"➖ Table removed: {table}")
            else:
                # Check columns
                old_cols = {c['name']: c for c in old_tables[table]['columns']}
                new_cols = {c['name']: c for c in new_tables[table]['columns']}
                
                for col in set(old_cols) | set(new_cols):
                    if col not in old_cols:
                        drifts.append(f"➕ Column added: {table}.{col}")
                    elif col not in new_cols:
                        drifts.append(f"➖ Column removed: {table}.{col}")
                    elif old_cols[col]['type'] != new_cols[col]['type']:
                        drifts.append(f"🔄 Type changed: {table}.{col} ({old_cols[col]['type']} → {new_cols[col]['type']})")
        
        return drifts

def send_slack_alert(webhook_url, drifts):
    """Send drift alerts to Slack"""
    import urllib.request
    import urllib.error
    
    if not webhook_url or not drifts:
        return
    
    message = {
        "text": f"🚨 Schema Drift Alert: {len(drifts)} changes detected",
        "attachments": [{
            "color": "warning",
            "fields": [{"title": "Changes", "value": "\n".join(drifts[:10])}]
        }]
    }
    
    try:
        req = urllib.request.Request(
            webhook_url,
            data=json.dumps(message).encode('utf-8'),
            headers={'Content-Type': 'application/json'}
        )
        urllib.request.urlopen(req)
        print("📢 Slack alert sent")
    except:
        pass  # Silently fail if webhook is invalid

def main():
    parser = argparse.ArgumentParser(description='Detect schema drifts in Snowflake')
    parser.add_argument('action', choices=['snapshot', 'compare'],
                      help='snapshot: take snapshot, compare: detect drifts')
    parser.add_argument('--database', required=True, help='Database name')
    parser.add_argument('--schema', required=True, help='Schema name')
    parser.add_argument('--save', help='Save snapshot to file')
    parser.add_argument('--baseline', help='Baseline file for comparison')
    parser.add_argument('--current', help='Current file for comparison')
    parser.add_argument('--webhook', help='Slack webhook URL for alerts')
    
    args = parser.parse_args()
    df = DriftForce()
    
    if args.action == 'snapshot':
        snapshot = df.snapshot(args.database, args.schema)
        if args.save:
            with open(args.save, 'w') as f:
                json.dump(snapshot, f, indent=2)
            print(f"💾 Saved to {args.save}")
        else:
            print(json.dumps(snapshot, indent=2))
    
    elif args.action == 'compare':
        if not args.baseline or not args.current:
            # Live comparison
            print("Taking baseline snapshot...")
            baseline = df.snapshot(args.database, args.schema)
            input("Make schema changes in Snowflake, then press Enter...")
            print("Taking current snapshot...")
            current = df.snapshot(args.database, args.schema)
        else:
            # File comparison
            with open(args.baseline) as f:
                baseline = json.load(f)
            with open(args.current) as f:
                current = json.load(f)
        
        drifts = df.detect_drifts(baseline, current)
        
        if drifts:
            print(f"\n🚨 Found {len(drifts)} drift(s):\n")
            for drift in drifts:
                print(f"  {drift}")
            
            # Send Slack alert if webhook provided
            if args.webhook:
                send_slack_alert(args.webhook, drifts)
        else:
            print("\n✅ No drifts detected")

if __name__ == '__main__':
    main()
