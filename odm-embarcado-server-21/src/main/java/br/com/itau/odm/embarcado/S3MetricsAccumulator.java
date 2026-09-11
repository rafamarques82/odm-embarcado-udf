package br.com.itau.odm.embarcado;

import org.apache.spark.util.AccumulatorV2;
import java.io.Serializable;

/**
 * Accumulator customizado para agregar métricas ODM de todos os executors Spark.
 * Thread-safe e serializável.
 */
public class S3MetricsAccumulator extends AccumulatorV2<S3MetricsAccumulator.MetricsData, S3MetricsAccumulator.MetricsData> {
    
    private MetricsData data = new MetricsData();
    
    public static class MetricsData implements Serializable {
        private static final long serialVersionUID = 1L;
        
        public long totalCount = 0;
        public long okCount = 0;
        public long errorCount = 0;
        public long totalDurationMs = 0;
        public long totalRulesFired = 0;
        public String rulesetPath = "(unknown)";
        public long startTimestampMs = 0;
        public long endTimestampMs = 0;
        
        public MetricsData() {}
        
        public MetricsData copy() {
            MetricsData copy = new MetricsData();
            copy.totalCount = this.totalCount;
            copy.okCount = this.okCount;
            copy.errorCount = this.errorCount;
            copy.totalDurationMs = this.totalDurationMs;
            copy.totalRulesFired = this.totalRulesFired;
            copy.rulesetPath = this.rulesetPath;
            copy.startTimestampMs = this.startTimestampMs;
            copy.endTimestampMs = this.endTimestampMs;
            return copy;
        }
        
        public void merge(MetricsData other) {
            this.totalCount += other.totalCount;
            this.okCount += other.okCount;
            this.errorCount += other.errorCount;
            this.totalDurationMs += other.totalDurationMs;
            this.totalRulesFired += other.totalRulesFired;
            
            if (other.rulesetPath != null && !other.rulesetPath.equals("(unknown)")) {
                this.rulesetPath = other.rulesetPath;
            }
            
            if (this.startTimestampMs == 0 || (other.startTimestampMs > 0 && other.startTimestampMs < this.startTimestampMs)) {
                this.startTimestampMs = other.startTimestampMs;
            }
            
            if (other.endTimestampMs > this.endTimestampMs) {
                this.endTimestampMs = other.endTimestampMs;
            }
        }
    }
    
    @Override
    public boolean isZero() {
        return data.totalCount == 0;
    }
    
    @Override
    public AccumulatorV2<MetricsData, MetricsData> copy() {
        S3MetricsAccumulator newAcc = new S3MetricsAccumulator();
        newAcc.data = this.data.copy();
        return newAcc;
    }
    
    @Override
    public void reset() {
        data = new MetricsData();
    }
    
    @Override
    public void add(MetricsData v) {
        data.merge(v);
    }
    
    @Override
    public void merge(AccumulatorV2<MetricsData, MetricsData> other) {
        data.merge(other.value());
    }
    
    @Override
    public MetricsData value() {
        return data.copy();
    }
    
    // Método helper para adicionar uma execução
    public void recordExecution(String rulesetPath, long durationMs, int rulesFired, boolean success) {
        MetricsData update = new MetricsData();
        update.totalCount = 1;
        update.okCount = success ? 1 : 0;
        update.errorCount = success ? 0 : 1;
        update.totalDurationMs = durationMs;
        update.totalRulesFired = rulesFired;
        update.rulesetPath = rulesetPath;
        update.startTimestampMs = System.currentTimeMillis();
        update.endTimestampMs = System.currentTimeMillis();
        add(update);
    }
}

// Made with Bob
