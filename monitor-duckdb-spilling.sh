#!/bin/bash
# Script to monitor DuckDB spilling in Kubernetes pod

POD_NAME=$(kubectl get pods -n default | grep transformation-silver | grep Running | head -1 | awk '{print $1}')

if [ -z "$POD_NAME" ]; then
    echo "❌ No running transformation-silver pod found!"
    exit 1
fi

echo "🔍 Monitoring DuckDB spilling in pod: $POD_NAME"
echo "================================================"
echo ""

# Check temp directory
echo "📁 DuckDB temp directory contents:"
kubectl exec -n default $POD_NAME -- ls -lh /tmp/duckdb/ 2>/dev/null || echo "   (empty or not created yet)"
echo ""

# Check disk usage
echo "💾 Disk usage in /tmp:"
kubectl exec -n default $POD_NAME -- df -h /tmp
echo ""

# Monitor temp files in real-time (run this in a loop)
echo "🔄 Watching for temp files (Ctrl+C to stop)..."
echo ""

while true; do
    TEMP_FILES=$(kubectl exec -n default $POD_NAME -- find /tmp/duckdb -type f 2>/dev/null | wc -l)
    TEMP_SIZE=$(kubectl exec -n default $POD_NAME -- du -sh /tmp/duckdb 2>/dev/null | awk '{print $1}')
    
    if [ "$TEMP_FILES" -gt 0 ]; then
        echo "✅ [$(date '+%H:%M:%S')] Spilling detected! Files: $TEMP_FILES, Size: $TEMP_SIZE"
        kubectl exec -n default $POD_NAME -- ls -lh /tmp/duckdb/
    else
        echo "⏳ [$(date '+%H:%M:%S')] No spilling yet... (temp files: $TEMP_FILES)"
    fi
    
    sleep 5
done
