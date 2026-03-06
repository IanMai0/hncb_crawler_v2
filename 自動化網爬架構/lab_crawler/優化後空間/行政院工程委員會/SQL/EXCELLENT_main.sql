CREATE TABLE vendor_status (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    vendor_id BIGINT NOT NULL,
    status_type ENUM('REJECT', 'EXCELLENT') NOT NULL,

    status_reason TEXT,
    publish_date DATE,
    expire_date DATE,

    is_active BOOLEAN DEFAULT TRUE,

    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (vendor_id) REFERENCES vendor(vendor_id),

    INDEX idx_vendor_status (vendor_id, status_type),
    INDEX idx_active (is_active)
);
