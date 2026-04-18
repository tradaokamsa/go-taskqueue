-- Create job status enum                                                            
CREATE TYPE job_status AS ENUM (                                                     
    'pending',
    'scheduled',                                                                     
    'running',                                                                       
    'completed',                                                                     
    'failed',                                                                        
    'cancelled',                                                                     
    'dead'                                                                           
);     

-- Create jobs table
CREATE TABLE jobs (                                                                  
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),                        
    type          VARCHAR(255) NOT NULL,                                             
    priority      INTEGER NOT NULL DEFAULT 0,                                        
    status        job_status NOT NULL DEFAULT 'pending',                             
    payload       JSONB,                                                             
    constraints   JSONB,                                                             
    result        JSONB,                                                             
    error         TEXT,                                                              
                                                                                    
    max_retries   INTEGER NOT NULL DEFAULT 3,                                        
    attempt       INTEGER NOT NULL DEFAULT 0,
    timeout_sec   INTEGER NOT NULL DEFAULT 3600,                                     
                                                                                    
    worker_id     VARCHAR(255),   

    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),                                
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),      
    scheduled_at  TIMESTAMPTZ,                                                       
    started_at    TIMESTAMPTZ,                                                       
    completed_at  TIMESTAMPTZ                           
);         

-- Indexes for common queries                                                        
CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_type_status ON jobs(type, status);                             
CREATE INDEX idx_jobs_scheduled_at ON jobs(scheduled_at) WHERE scheduled_at IS NOT NULL;                                                                                
                                                                                    
-- Partial index for pending jobs ordered by priority (most important!!!)              
CREATE INDEX idx_jobs_pending_priority ON jobs(priority DESC, created_at ASC) WHERE status = 'pending';   