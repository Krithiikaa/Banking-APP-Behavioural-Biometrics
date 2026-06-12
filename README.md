# Banking-APP-Behavioural-Biometrics

# 🏦 Behavioural Biometrics — System Architecture & Implementation Plan

> **Project**: Continuous Authentication via Behavioural Biometrics for Banking  
> **Base App**: HDFC Bank Clone (Android + Desktop)  
> **Core Idea**: Capture 39 sensor signals from the user's device during banking sessions, train a per-user behavioural model, and score every session in real-time to detect impostors.

---
## 🚀 Live Preview

> • [🎨 System Design ⚡ - Live Demo ](https://system-design-behavioural-biometrics.edgeone.app/) •
 
> • [🎨 Architecture ⚡ - Live Demo ](https://krithiikaa.github.io/Banking-APP-Behavioural-Biometrics/) •

> • [🎨 Mobile UI Design ⚡ - Live Demo ](https://stitch.withgoogle.com/preview/10837085091759448495?node-id=9fba777b8baf453395c5302f181209c6) •

> • [🎨 Desktop UI Design ⚡ - Live Demo ](https://stitch.withgoogle.com/preview/5980981783955325194?node-id=1cc9328b12a441349fd81d3c5f37d7cb) •

---

## 1. High-Level System Design Diagram

```mermaid
graph TB
    subgraph CLIENT["📱 CLIENT LAYER"]
        direction TB
        A1["Android App<br/>(HDFC Bank Clone)<br/>Jetpack Compose / React Native"]
        A2["Desktop App<br/>(HDFC NetBanking Clone)<br/>Electron / React"]
    end

    subgraph EDGE["⚡ EDGE PROCESSING (On-Device)"]
        direction TB
        E1["Sensor Abstraction Layer<br/>39 Sensors (Android) / 15+ Signals (Desktop)"]
        E2["Feature Extractor<br/>2s sliding window → 300 floats"]
        E3["Privacy Filter<br/>GPS→Geohash, Mic→Energy only"]
        E4["Local Risk Pre-Screener<br/>TFLite / ONNX on-device model"]
        E5["Event Buffer<br/>SQLite (Android) / IndexedDB (Desktop)"]
    end

    subgraph TRANSPORT["🔒 SECURE TRANSPORT"]
        T1["HTTPS/2 Batch Upload<br/>JWT + Device Attestation"]
        T2["WebSocket (WSS)<br/>Real-time decision push"]
    end

    subgraph BACKEND["☁️ BACKEND MICROSERVICES"]
        direction TB
        B1["API Gateway — Kong<br/>Auth, Rate Limit, TLS"]
        B2["Collection Service — Go<br/>Schema validation, dedup"]
        B3["Flink Stream Processor<br/>30s session windows, enrichment"]
        B4["Inference Service — TorchServe<br/>Per-user LSTM scoring"]
        B5["Risk Engine — FastAPI<br/>ML + Rules + Device Trust fusion"]
        B6["Session Manager — Node.js<br/>WebSocket push to clients"]
    end

    subgraph STORAGE["💾 DATA STORES"]
        direction TB
        S1["Kafka (MSK)<br/>Event streaming backbone"]
        S2["TimescaleDB<br/>Time-series feature vectors"]
        S3["PostgreSQL<br/>User profiles, audit log"]
        S4["Redis Cluster<br/>Model cache, session state"]
        S5["S3 / Object Storage<br/>Model artifacts, raw backups"]
        S6["Elasticsearch<br/>Fraud ops search & dashboards"]
    end

    subgraph ML["🧠 ML PLATFORM"]
        direction TB
        M1["MLflow Model Registry<br/>Versioned per-user models"]
        M2["Airflow DAG<br/>Nightly retraining scheduler"]
        M3["Training Pipeline<br/>LSTM Autoencoder per user"]
    end

    subgraph DECISION["🛡️ DECISION & GOVERNANCE"]
        direction TB
        D1["Risk Decision<br/>ALLOW / STEP-UP / BLOCK"]
        D2["Audit Ledger<br/>Immutable append-only log"]
        D3["HITL Dashboard<br/>Fraud ops review & override"]
    end

    A1 --> E1
    A2 --> E1
    E1 --> E2 --> E3 --> E4 --> E5
    E5 --> T1
    T1 --> B1 --> B2
    B2 --> S1
    S1 --> B3
    B3 --> S1
    S1 --> B4
    B4 --> S4
    B4 --> M1
    B4 --> S1
    S1 --> B5
    B5 --> S1
    S1 --> B6
    B6 --> T2
    T2 --> A1
    T2 --> A2
    B3 --> S2
    B5 --> S3
    B5 --> D1
    D1 --> D2
    D1 --> D3
    M2 --> M3
    M3 --> M1
    M3 --> S2
    B5 --> S6

    style CLIENT fill:#E6F1FB,stroke:#85B7EB,color:#0C447C
    style EDGE fill:#FAEEDA,stroke:#EF9F27,color:#633806
    style TRANSPORT fill:#F1EFE8,stroke:#B4B2A9,color:#444441
    style BACKEND fill:#EEEDFE,stroke:#AFA9EC,color:#3C3489
    style STORAGE fill:#E1F5EE,stroke:#5DCAA5,color:#085041
    style ML fill:#EAF3DE,stroke:#97C459,color:#27500A
    style DECISION fill:#FAECE7,stroke:#F0997B,color:#712B13
```

---

## 2. Android App Architecture (HDFC Bank Clone + 39 Sensors)

```mermaid
graph TD
    subgraph UI["📱 UI LAYER — HDFC Bank Clone"]
        U1["Login Screen<br/>Typing cadence capture from password field"]
        U2["Dashboard<br/>Touch heatmap, scroll velocity, navigation path"]
        U3["Fund Transfer<br/>Keystroke timing on amount, swipe-to-confirm"]
        U4["Settings / Profile<br/>Session duration, orientation events"]
    end

    subgraph SAL["🔧 SENSOR ABSTRACTION LAYER (Background Service)"]
        direction LR
        S1["MotionCollector<br/>Accelerometer + Gyroscope<br/>+ Rotation Vector<br/>(50-100 Hz)"]
        S2["TouchCollector<br/>X/Y/Pressure/Size<br/>per pointer via<br/>MotionEvent hooks"]
        S3["LocationCollector<br/>GPS (1Hz), Wi-Fi RSSI<br/>BLE RSSI, Cell Tower"]
        S4["EnvCollector<br/>Light, Proximity<br/>Barometer, Mic Energy"]
        S5["BiometricCollector<br/>Fingerprint events<br/>Face unlock, PPG"]
        S6["SystemCollector<br/>Screen on/off, Battery<br/>App foreground changes"]
    end

    subgraph EDGE["⚡ EDGE PROCESSING LAYER"]
        FE["Feature Extractor<br/>mean, std, FFT, DTW<br/>→ 300 float vector per 2s window"]
        LR2["Local Risk Scorer<br/>TFLite model for<br/>on-device pre-screening"]
        BUF["Event Buffer<br/>SQLite ring buffer (10MB)<br/>Retry queue for uploads"]
        PF["Privacy Filter<br/>GPS→geohash-6, Mic→energy<br/>Differential noise ε=0.1"]
    end

    subgraph NET["🌐 NETWORK TRANSPORT"]
        HTTPS["HTTPS/2 Batch Upload<br/>POST /api/v1/biometric/events<br/>JWT + Play Integrity token"]
        WSS["WebSocket (WSS)<br/>Real-time risk decisions<br/>ALLOW | STEP_UP | BLOCK"]
    end

    subgraph SEC["🔐 SECURITY LAYER"]
        TEE["TEE / Android Keystore<br/>Hardware-backed biometric keys"]
        ENC["AES-256-GCM Encryption<br/>Local data at rest"]
        ROOT["Root/Emulator Detection<br/>SafetyNet, Frida, Magisk checks"]
    end

    UI --> SAL
    SAL --> EDGE
    FE --> LR2
    LR2 --> PF
    PF --> BUF
    BUF --> HTTPS
    WSS --> UI
    SEC -.->|protects| EDGE
    SEC -.->|protects| NET

    style UI fill:#E6F1FB,stroke:#85B7EB
    style SAL fill:#EEEDFE,stroke:#AFA9EC
    style EDGE fill:#FAEEDA,stroke:#EF9F27
    style NET fill:#E1F5EE,stroke:#5DCAA5
    style SEC fill:#FAECE7,stroke:#F0997B
```

### The 39 Sensors (Android)

| # | Category | Sensors | Hz | Features Extracted |
|---|----------|---------|----|--------------------|
| 1-3 | **Motion** | Accelerometer (X,Y,Z) | 100 | Mean, std, FFT peaks, jerk |
| 4-6 | **Motion** | Gyroscope (X,Y,Z) | 100 | Angular velocity, tilt pattern |
| 7-9 | **Motion** | Rotation Vector (X,Y,Z) | 50 | Device orientation signature |
| 10 | **Motion** | Step Counter | 1 | Gait pattern during phone use |
| 11-14 | **Touch** | X, Y, Pressure, Size | Event | Touch heatmap, swipe shape (DTW) |
| 15-16 | **Touch** | Velocity X, Velocity Y | Event | Swipe speed signature |
| 17-18 | **Touch** | Touch area, Pointer count | Event | Grip pattern, multi-touch habits |
| 19 | **Location** | GPS lat/lon | 1 | Usual locations (geohash) |
| 20-21 | **Location** | Wi-Fi RSSI, SSID | 0.03 | Known networks proximity |
| 22-23 | **Location** | BLE RSSI, Cell Tower ID | 0.1 | Indoor location context |
| 24 | **Environment** | Ambient Light | 5 | Indoor/outdoor, time-of-day |
| 25 | **Environment** | Proximity | 5 | Phone-to-face distance |
| 26 | **Environment** | Barometer | 1 | Floor level, altitude |
| 27 | **Environment** | Mic Energy Level | 10 | Background noise fingerprint |
| 28-29 | **Biometric** | Fingerprint pass/fail, Face unlock | Event | Auth method preference |
| 30 | **Biometric** | PPG / Heart Rate | 1 | Stress level during transactions |
| 31-33 | **Keyboard** | Dwell time, Flight time, Error rate | Event | Typing rhythm signature |
| 34-35 | **System** | Screen on/off, App foreground | Event | Usage pattern |
| 36 | **System** | Battery level + charging | 0.1 | Charging habits |
| 37 | **System** | Device orientation | Event | Portrait/landscape preference |
| 38 | **Network** | Connection type (WiFi/LTE) | 0.1 | Network behavior |
| 39 | **Temporal** | Time-of-day + day-of-week | Per-session | Usage schedule fingerprint |

---

## 3. Desktop App Architecture (HDFC NetBanking Clone)

```mermaid
graph TD
    subgraph UI["🖥️ UI LAYER — HDFC NetBanking Clone"]
        U1["Login Page<br/>Keystroke dwell + flight time capture"]
        U2["Dashboard / Transfers<br/>Mouse trajectories, form fill order"]
        U3["Invisible Overlay (Win32)<br/>Low-level KB/Mouse hooks"]
        U4["Browser Extension Mode<br/>Chrome/FF content script injection"]
    end

    subgraph SAL["🔧 SENSOR ABSTRACTION LAYER — Desktop Signals"]
        direction LR
        S1["Keyboard Dynamics<br/>Dwell, flight, digraph/trigraph<br/>~8 features per keystroke"]
        S2["Mouse Dynamics<br/>Velocity, acceleration, curvature<br/>Click pressure, Bezier curves"]
        S3["Device Motion<br/>MacBook accelerometer<br/>via CMMotionManager"]
        S4["Camera (WebRTC)<br/>Face presence, blink rate<br/>Gaze direction (MediaPipe)"]
        S5["Audio (Web Audio)<br/>Mic energy pattern<br/>KB click acoustics"]
        S6["Network & Environment<br/>IP geoloc, Wi-Fi SSID<br/>Screen res, timezone"]
        S7["Browser Fingerprint<br/>Canvas, WebGL, AudioContext<br/>Font list fingerprint"]
        S8["Session Behavior<br/>Tab switches, idle intervals<br/>Copy-paste vs typing"]
    end

    subgraph EDGE["⚡ EDGE PROCESSING"]
        WW["Web Worker / Electron Main Process<br/>Off-thread feature extraction"]
        IDB["IndexedDB Ring Buffer<br/>60s feature vector retention"]
        PF["Privacy Filter<br/>IP→/24 subnet, Camera→boolean"]
    end

    subgraph NET["🌐 TRANSPORT (Same as Android)"]
        HTTPS["HTTPS/2 → Same Collection API"]
        WSS["WSS → Same Session Manager"]
    end

    UI --> SAL --> EDGE --> NET
    WSS --> UI

    style UI fill:#E6F1FB,stroke:#85B7EB
    style SAL fill:#EEEDFE,stroke:#AFA9EC
    style EDGE fill:#FAEEDA,stroke:#EF9F27
    style NET fill:#E1F5EE,stroke:#5DCAA5
```

### Desktop vs Android — Key Differences

| Aspect | Android (39 sensors) | Desktop (15+ signals) |
|--------|---------------------|-----------------------|
| **Motion** | Accel, Gyro, Rotation (hardware) | Only MacBook/Surface (limited) |
| **Location** | GPS, BLE, Cell Tower, Wi-Fi | IP Geolocation only (~city) |
| **Touch** | Full multitouch with pressure | Mouse only (richer trajectory) |
| **Keyboard** | ~5-10 features (touchscreen) | ~50-80 features (physical keys) |
| **Biometrics** | Fingerprint, Face, PPG | Windows Hello / Touch ID (opaque) |
| **Strongest Signal** | Motion + Touch + Location | Keyboard + Mouse dynamics |
| **Edge Runtime** | TFLite + SQLite | Web Worker + IndexedDB |

---

## 4. Backend Microservices & Storage Architecture

```mermaid
graph LR
    subgraph INGRESS["API Gateway (Kong)"]
        GW["JWT validation<br/>Rate limiting<br/>Device attestation<br/>TLS termination"]
    end

    subgraph SERVICES["Microservices"]
        CS["Collection Service<br/>(Go)<br/>Schema validation<br/>Dedup via Redis"]
        FP["Flink Processor<br/>30s session windows<br/>Session-level features"]
        IS["Inference Service<br/>(TorchServe)<br/>Per-user LSTM scoring"]
        RE["Risk Engine<br/>(FastAPI)<br/>ML + Rules fusion"]
        SM["Session Manager<br/>(Node.js)<br/>WSS push to clients"]
    end

    subgraph DATA["Data Stores"]
        KF["Kafka (MSK)<br/>5 topics, 7d retention<br/>3x replication"]
        TS["TimescaleDB<br/>Feature vectors<br/>90d raw, 1y aggregated"]
        PG["PostgreSQL<br/>Profiles, rules, audit<br/>Append-only audit log"]
        RD["Redis Cluster<br/>Model cache (TTL 30m)<br/>Session state, dedup"]
        S3["S3<br/>Model .pt files<br/>Encrypted raw backups"]
        ES["Elasticsearch<br/>Fraud ops search<br/>Kibana dashboards"]
    end

    GW --> CS --> KF
    KF --> FP --> KF
    FP --> TS
    KF --> IS
    IS --> RD
    IS --> KF
    KF --> RE
    RE --> PG
    RE --> KF
    KF --> SM
    IS -.-> S3
    RE -.-> ES

    style INGRESS fill:#F1EFE8,stroke:#B4B2A9
    style SERVICES fill:#EEEDFE,stroke:#AFA9EC
    style DATA fill:#E1F5EE,stroke:#5DCAA5
```

### Kafka Topics

| Topic | Producer | Consumer | Purpose |
|-------|----------|----------|---------|
| `biometric.events.raw` | Collection Service | Flink | Raw feature vectors from devices |
| `biometric.features.enriched` | Flink | Inference Service | Session-level enriched features |
| `biometric.scores` | Inference Service | Risk Engine | ML risk scores + anomaly flags |
| `biometric.decisions` | Risk Engine | Session Manager | ALLOW / STEP_UP / BLOCK |
| `audit.trail` | Risk Engine | Elasticsearch | Immutable audit events |

---

## 5. ML Training & Prediction Pipeline

```mermaid
graph TD
    subgraph ENROLL["📝 ENROLLMENT (Sessions 1-5)"]
        EN1["User performs normal banking<br/>Login, balance, transfer, logout"]
        EN2["Feature vectors collected<br/>Labeled: GENUINE"]
        EN3["Stored in TimescaleDB<br/>Tagged: enrollment_data=true"]
        EN4["LSTM Autoencoder trained<br/>Input: 300 floats → Reconstruct"]
        EN5["Model → MLflow Registry<br/>.pt file + scaler + threshold"]
        EN6["Baseline profile → Redis<br/>Per-sensor mean/std, usual locations"]
    end

    subgraph INFER["🔍 LIVE INFERENCE (Every 2 seconds)"]
        IN1["Feature vector arrives<br/>from Flink enriched stream"]
        IN2["Load user model<br/>Redis cache (2ms) or S3 (50ms)"]
        IN3["LSTM Autoencoder forward pass<br/>Compute reconstruction error (MSE)"]
        IN4["Per-sensor anomaly flags<br/>Features exceeding 3σ from baseline"]
        IN5["Risk Score Computation<br/>0.6×recon_error + 0.25×anomaly_ratio<br/>+ 0.15×context_risk"]
        IN6["Decision Thresholds<br/>< 0.30 ALLOW | 0.30-0.70 STEP_UP<br/>> 0.70 BLOCK"]
    end

    subgraph RETRAIN["🔄 CONTINUOUS RETRAINING"]
        RT1["Confirmed-genuine sessions<br/>OTP passed, no fraud report 48h"]
        RT2["Nightly Airflow DAG<br/>Fine-tune with new data, LR=0.0001"]
        RT3["Validation gate<br/>Must beat current model on holdout"]
        RT4["Canary deploy 10% → 24h<br/>If no FP surge → promote 100%"]
        RT5["Drift detection<br/>7-day rising error → re-enrollment"]
    end

    EN1 --> EN2 --> EN3 --> EN4 --> EN5 --> EN6
    EN6 -->|"Enrollment complete"| IN1
    IN1 --> IN2 --> IN3 --> IN4 --> IN5 --> IN6

    IN6 -->|"Confirmed genuine sessions"| RT1
    RT1 --> RT2 --> RT3 --> RT4
    RT4 -->|"Updated model"| IN2
    RT3 -->|"Failed validation"| RT2
    RT5 -->|"Force re-enrollment"| EN1

    style ENROLL fill:#EAF3DE,stroke:#97C459
    style INFER fill:#E1F5EE,stroke:#5DCAA5
    style RETRAIN fill:#FAEEDA,stroke:#EF9F27
```

### Risk Score → Decision Matrix

| Score Range | Decision | Action |
|-------------|----------|--------|
| **0.00 – 0.30** | ✅ ALLOW | Transparent. No user interruption. Logged to audit. |
| **0.30 – 0.55** | 🟡 SOFT FLAG | Log only. Visible to fraud ops. Used for retraining. |
| **0.55 – 0.70** | 🟠 STEP-UP AUTH | Silent biometric re-auth → if fail, OTP to mobile. |
| **0.70 – 0.85** | 🔴 HIGH-RISK STEP-UP | Biometric + OTP. Maker-checker for transfers > ₹1L. |
| **0.85 – 1.00** | 🚫 BLOCK | Session terminated. Account flagged. Ops alert + SMS. |

---

## 6. End-to-End Data Flow (Sensor → Decision in <200ms)

```mermaid
sequenceDiagram
    participant Phone as 📱 Android/Desktop
    participant Edge as ⚡ Edge (On-Device)
    participant API as ☁️ Collection API
    participant Kafka as 📨 Kafka
    participant Flink as ⚙️ Flink
    participant ML as 🧠 Inference
    participant Risk as 🛡️ Risk Engine
    participant WS as 🔌 Session Mgr

    Phone->>Edge: Raw sensor events (39 signals, 1-200 Hz)
    Note over Edge: 2s sliding window<br/>Feature extraction → 300 floats<br/>Privacy filter applied
    Edge->>API: HTTPS/2 batch (JWT + attestation)
    API->>Kafka: biometric.events.raw
    Kafka->>Flink: Stream consume
    Note over Flink: 30s session window<br/>Session-level features<br/>Device trust enrichment
    Flink->>Kafka: biometric.features.enriched
    Kafka->>ML: Stream consume
    Note over ML: Load user LSTM (Redis cache)<br/>Reconstruction error → risk_score<br/>Per-sensor anomaly flags
    ML->>Kafka: biometric.scores
    Kafka->>Risk: Stream consume
    Note over Risk: Fuse: 0.6×ML + 0.3×Rules + 0.1×Trust<br/>→ ALLOW / STEP_UP / BLOCK
    Risk->>Kafka: biometric.decisions
    Risk->>Kafka: audit.trail
    Kafka->>WS: Stream consume
    WS->>Phone: WSS push (decision)
    Note over Phone: Enforce: continue / re-auth / block
```

---

## 7. Mapping to Current Project Code

Your existing project already implements a simplified version of this system:

| System Component | Current Implementation | Production Target |
|-----------------|----------------------|-------------------|
| **Sensor Collection** | [collect_data.py](file:///home/krithiii/Downloads/Precision-biometric-Intern/collect_data.py) — Keystroke hold/flight times | 39 sensor SAL service |
| **Feature Storage** | `data/typing_data.csv` — CSV file | TimescaleDB + Kafka |
| **Model Training** | [train_model.py](file:///home/krithiii/Downloads/Precision-biometric-Intern/train_model.py) — RandomForest classifier | LSTM Autoencoder + Airflow |
| **Prediction** | [predict.py](file:///home/krithiii/Downloads/Precision-biometric-Intern/predict.py) — Binary genuine/impostor | Risk score 0.0–1.0 + anomaly flags |
| **Model Artifacts** | `model.pkl`, `scaler.pkl` — Pickle files | MLflow registry + S3 |
| **Features** | 15 features (8 hold + 7 flight times) | ~300 features per 2s window |
| **Decision** | Binary: Authenticated / Denied | 5-tier: ALLOW → BLOCK |

---

## 8. Implementation Plan (Phased)

### Phase 1: Foundation 
> Expand keystroke collection + build Android UI

- [ ] Clone HDFC Bank app UI (React Native / Jetpack Compose)
- [ ] Implement login screen with keystroke dynamics capture
- [ ] Build Sensor Abstraction Layer for motion sensors (Accel, Gyro)
- [ ] Set up SQLite ring buffer for on-device storage
- [ ] Expand feature vector from 15 → 50+ features

### Phase 2: Full Sensor Suite 
> Add all 39 sensors + edge processing

- [ ] Add TouchCollector, LocationCollector, EnvCollector
- [ ] Implement 2-second sliding window feature extractor
- [ ] Build privacy filter (GPS→geohash, mic→energy)
- [ ] Add TFLite on-device pre-screener
- [ ] Implement root/emulator detection

### Phase 3: Backend Pipeline
> Set up streaming infrastructure

- [ ] Deploy Kafka cluster with 5 topics
- [ ] Build Collection Service (Go) with schema validation
- [ ] Deploy Flink stream processor for session windowing
- [ ] Set up TimescaleDB for feature vector storage
- [ ] Configure API Gateway (Kong) with JWT + rate limiting

### Phase 4: ML Platform 
> Replace RandomForest with LSTM Autoencoder

- [ ] Train per-user LSTM Autoencoder on enrollment data
- [ ] Set up MLflow model registry
- [ ] Build inference service (TorchServe) with Redis model cache
- [ ] Implement risk score computation (reconstruction error based)
- [ ] Deploy Airflow DAG for nightly retraining

### Phase 5: Risk Engine & Decisions
> Fuse ML + rules + device trust

- [ ] Build Risk Engine (FastAPI) with decision fusion
- [ ] Implement 5-tier decision matrix (ALLOW → BLOCK)
- [ ] Set up WebSocket session manager for real-time push
- [ ] Build immutable audit log (append-only PostgreSQL)
- [ ] Integrate step-up auth flows (biometric + OTP)

### Phase 6: Desktop App 
> Port to Electron / browser extension

- [ ] Build HDFC NetBanking clone (Electron + React)
- [ ] Implement keyboard/mouse dynamics collectors
- [ ] Add browser fingerprinting (Canvas, WebGL)
- [ ] Reuse same backend pipeline (shared Collection API)
- [ ] Validate with desktop-specific ML features

### Phase 7: HITL & Governance 
> Human-in-the-loop + compliance

- [ ] Build fraud ops dashboard (Kibana + custom React)
- [ ] Implement maker-checker for high-value transfers
- [ ] Add model governance (validation gate, canary deploy)
- [ ] Set up drift detection + forced re-enrollment
- [ ] Implement right-to-explanation for blocked transactions
- [ ] Audit log retention policies (RBI: 7 years)

### Phase 8: Hardening & Launch 
> Security, performance, compliance

- [ ] Penetration testing + security audit
- [ ] Load testing (target: 5000 concurrent users, <200ms E2E)
- [ ] GDPR / RBI compliance review
- [ ] False positive rate monitoring + alerting
- [ ] Production deployment with canary rollout

---

> [!IMPORTANT]
> The existing codebase ([collect_data.py](file:///home/krithiii/Downloads/Precision-biometric-Intern/collect_data.py), [train_model.py](file:///home/krithiii/Downloads/Precision-biometric-Intern/train_model.py), [predict.py](file:///home/krithiii/Downloads/Precision-biometric-Intern/predict.py)) is a **proof-of-concept** for Phase 1. It demonstrates the core keystroke biometrics pipeline — the production system scales this to 39 sensors, streaming infrastructure, and real-time ML inference.
