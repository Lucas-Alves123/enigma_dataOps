import os
import random
from datetime import datetime
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import List, Optional

from consumer.receiver import ConsumerReceiver
from bronze.processor import BronzeProcessor
from silver.processor import SilverProcessor
from gold.processor import GoldProcessor

app = FastAPI(
    title="Medallion Pipeline API",
    version="1.0.0"
)

db_received_data: List[dict] = []

class GoldRecord(BaseModel):
    pipeline_run_id: str
    full_payload: str
    assembled_parts_count: int
    min_index: int
    max_index: int
    missing_indices: List[int]
    data_integrity_status: str
    processed_at: str

@app.post("/api/data", response_model=dict, status_code=201)
def receive_gold_data(record: GoldRecord):
    record_dict = record.model_dump()
    record_dict["received_at_api"] = datetime.now().isoformat()
    db_received_data.insert(0, record_dict)
    print(f"[API] Received Gold Run: {record.pipeline_run_id}")
    return {"status": "sucesso", "resposta": record.full_payload}

@app.get("/api/data", response_model=List[dict])
def get_gold_data():
    return db_received_data

@app.post("/api/trigger")
def trigger_pipeline():
    base_dir = "data"
    bronze_proc = BronzeProcessor(base_dir)
    silver_proc = SilverProcessor(base_dir, bronze_proc)
    gold_proc = GoldProcessor(base_dir, silver_proc, api_url="http://127.0.0.1:8000/api/data")

    bronze_proc.clear()
    silver_proc.clear()
    gold_proc.clear()

    fragments = [
        {"part_index": 1, "payload": "Olá"},
        {"part_index": 2, "payload": "mundo"},
        {"part_index": 3, "payload": ", este é o enigma"},
        {"part_index": 4, "payload": "!"}
    ]

    duplicates = [
        {"part_index": 2, "payload": "mundo"}
    ]
    
    raw_stream = fragments + duplicates
    random.shuffle(raw_stream)

    print("[API Trigger] Simulated stream:")
    for idx, item in enumerate(raw_stream):
        print(f"  Item {idx + 1}: Index = {item['part_index']} | Payload = '{item['payload']}'")

    consumer = ConsumerReceiver(bronze_proc)
    ingested_files = []
    for item in raw_stream:
        filepath = consumer.receive_message(item)
        ingested_files.append(filepath)

    silver_records, silver_file = silver_proc.process_bronze_data()
    gold_record = gold_proc.process_silver_data()
    api_success = gold_proc.send_to_api(gold_record)

    return {
        "status": "success",
        "producer": {
            "emitted_events_count": len(raw_stream)
        },
        "consumer_bronze": {
            "ingested_files_count": len(ingested_files)
        },
        "silver": {
            "cleaned_records_count": len(silver_records)
        },
        "gold": {
            "assembled_message": gold_record.get("full_payload", ""),
            "integrity_status": gold_record.get("data_integrity_status", ""),
            "api_integrated": api_success
        }
    }

@app.get("/", response_class=HTMLResponse)
def get_dashboard():
    html_content = """
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Enigma - Medallion Pipeline Dashboard</title>
        <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&family=Plus+Jakarta+Sans:wght@300;400;600;700&display=swap" rel="stylesheet">
        <style>
            :root {
                --bg-primary: #030712;
                --bg-secondary: #0b1528;
                --accent-blue: #3b82f6;
                --accent-cyan: #06b6d4;
                --color-bronze: #d97706;
                --color-silver: #94a3b8;
                --color-gold: #fbbf24;
                --color-success: #10b981;
                --text-main: #f3f4f6;
                --text-muted: #9ca3af;
                --glass-bg: rgba(15, 23, 42, 0.45);
                --glass-border: rgba(255, 255, 255, 0.08);
            }

            * {
                box-sizing: border-box;
                margin: 0;
                padding: 0;
            }

            body {
                font-family: 'Plus Jakarta Sans', sans-serif;
                background-color: var(--bg-primary);
                background-image: 
                    radial-gradient(circle at 10% 20%, rgba(59, 130, 246, 0.05) 0%, transparent 40%),
                    radial-gradient(circle at 90% 80%, rgba(6, 182, 212, 0.05) 0%, transparent 45%);
                background-attachment: fixed;
                color: var(--text-main);
                min-height: 100vh;
                padding: 2rem;
                display: flex;
                flex-direction: column;
                align-items: center;
            }

            header {
                width: 100%;
                max-width: 1200px;
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 2.5rem;
                border-bottom: 1px solid var(--glass-border);
                padding-bottom: 1.5rem;
            }

            h1 {
                font-family: 'Outfit', sans-serif;
                font-weight: 800;
                font-size: 2.2rem;
                background: linear-gradient(135deg, #3b82f6, #06b6d4, #10b981);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                letter-spacing: -0.5px;
            }

            .subtitle {
                color: var(--text-muted);
                font-size: 0.95rem;
                margin-top: 0.25rem;
            }

            .btn-trigger {
                background: linear-gradient(135deg, #2563eb, #0891b2);
                color: #ffffff;
                border: none;
                padding: 0.8rem 1.8rem;
                border-radius: 12px;
                font-weight: 600;
                font-size: 0.95rem;
                cursor: pointer;
                transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
                box-shadow: 0 4px 20px rgba(37, 99, 235, 0.25);
                display: flex;
                align-items: center;
                gap: 0.5rem;
            }

            .btn-trigger:hover {
                transform: translateY(-2px);
                box-shadow: 0 6px 25px rgba(6, 182, 212, 0.4);
                filter: brightness(1.1);
            }

            .btn-trigger:active {
                transform: translateY(0);
            }

            main {
                width: 100%;
                max-width: 1200px;
                display: flex;
                flex-direction: column;
                gap: 2.5rem;
            }

            .pipeline-flow {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                gap: 1.5rem;
                width: 100%;
                position: relative;
            }

            .flow-card {
                background: var(--glass-bg);
                border: 1px solid var(--glass-border);
                backdrop-filter: blur(12px);
                border-radius: 18px;
                padding: 1.5rem;
                text-align: center;
                transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
                display: flex;
                flex-direction: column;
                align-items: center;
                position: relative;
                overflow: hidden;
            }

            .flow-card::before {
                content: '';
                position: absolute;
                top: 0;
                left: 0;
                width: 100%;
                height: 4px;
                opacity: 0.8;
            }

            .card-producer::before { background: #f59e0b; }
            .card-producer:hover { box-shadow: 0 8px 30px rgba(245, 158, 11, 0.15); border-color: rgba(245, 158, 11, 0.3); }
            
            .card-consumer::before { background: var(--accent-cyan); }
            .card-consumer:hover { box-shadow: 0 8px 30px rgba(6, 182, 212, 0.15); border-color: rgba(6, 182, 212, 0.3); }

            .card-bronze::before { background: var(--color-bronze); }
            .card-bronze:hover { box-shadow: 0 8px 30px rgba(217, 119, 6, 0.15); border-color: rgba(217, 119, 6, 0.3); }

            .card-silver::before { background: var(--color-silver); }
            .card-silver:hover { box-shadow: 0 8px 30px rgba(148, 163, 184, 0.15); border-color: rgba(148, 163, 184, 0.3); }

            .card-gold::before { background: var(--color-gold); }
            .card-gold:hover { box-shadow: 0 8px 30px rgba(251, 191, 36, 0.15); border-color: rgba(251, 191, 36, 0.3); }

            .card-api::before { background: var(--color-success); }
            .card-api:hover { box-shadow: 0 8px 30px rgba(16, 185, 129, 0.15); border-color: rgba(16, 185, 129, 0.3); }

            .layer-badge {
                font-size: 0.7rem;
                text-transform: uppercase;
                font-weight: 800;
                letter-spacing: 1.5px;
                padding: 0.3rem 0.7rem;
                border-radius: 20px;
                margin-bottom: 1rem;
            }

            .badge-producer { background: rgba(245, 158, 11, 0.15); color: #f59e0b; }
            .badge-consumer { background: rgba(6, 182, 212, 0.15); color: var(--accent-cyan); }
            .badge-bronze { background: rgba(217, 119, 6, 0.15); color: var(--color-bronze); }
            .badge-silver { background: rgba(148, 163, 184, 0.15); color: var(--color-silver); }
            .badge-gold { background: rgba(251, 191, 36, 0.15); color: var(--color-gold); }
            .badge-api { background: rgba(16, 185, 129, 0.15); color: var(--color-success); }

            .card-title {
                font-size: 1.15rem;
                font-weight: 700;
                margin-bottom: 0.5rem;
            }

            .card-desc {
                font-size: 0.8rem;
                color: var(--text-muted);
                line-height: 1.4;
                text-align: center;
            }

            .dashboard-grid {
                display: grid;
                grid-template-columns: 1fr;
                gap: 2rem;
            }

            .panel {
                background: var(--glass-bg);
                border: 1px solid var(--glass-border);
                backdrop-filter: blur(12px);
                border-radius: 24px;
                padding: 2rem;
                width: 100%;
            }

            .panel-title {
                font-family: 'Outfit', sans-serif;
                font-weight: 700;
                font-size: 1.3rem;
                margin-bottom: 1.5rem;
                display: flex;
                align-items: center;
                justify-content: space-between;
                border-bottom: 1px solid var(--glass-border);
                padding-bottom: 0.75rem;
            }

            .run-table {
                width: 100%;
                border-collapse: collapse;
                margin-top: 1rem;
                text-align: left;
            }

            .run-table th {
                padding: 1rem;
                color: var(--text-muted);
                font-weight: 600;
                font-size: 0.85rem;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                border-bottom: 1px solid var(--glass-border);
            }

            .run-table td {
                padding: 1.2rem 1rem;
                border-bottom: 1px solid rgba(255, 255, 255, 0.04);
                font-size: 0.9rem;
            }

            .run-table tr:hover {
                background: rgba(255, 255, 255, 0.02);
            }

            .status-tag {
                display: inline-block;
                padding: 0.25rem 0.6rem;
                border-radius: 8px;
                font-size: 0.75rem;
                font-weight: 700;
            }

            .status-completed {
                background: rgba(16, 185, 129, 0.15);
                color: var(--color-success);
                border: 1px solid rgba(16, 185, 129, 0.3);
            }

            .status-incomplete {
                background: rgba(239, 68, 68, 0.15);
                color: #ef4444;
                border: 1px solid rgba(239, 68, 68, 0.3);
            }

            .empty-state {
                text-align: center;
                padding: 4rem 2rem;
                color: var(--text-muted);
            }

            .empty-state svg {
                margin-bottom: 1rem;
                opacity: 0.3;
            }

            .toast {
                position: fixed;
                bottom: 2rem;
                right: 2rem;
                background: #1e293b;
                border: 1px solid var(--accent-cyan);
                color: #fff;
                padding: 1rem 1.5rem;
                border-radius: 12px;
                box-shadow: 0 10px 25px rgba(0,0,0,0.3);
                transform: translateY(150%);
                transition: transform 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275);
                display: flex;
                align-items: center;
                gap: 0.75rem;
                z-index: 1000;
            }

            .toast.show {
                transform: translateY(0);
            }

            .pulsing-dot {
                width: 8px;
                height: 8px;
                background-color: var(--color-success);
                border-radius: 50%;
                box-shadow: 0 0 8px var(--color-success);
                animation: pulse 1.5s infinite;
            }

            @keyframes pulse {
                0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
                70% { transform: scale(1); box-shadow: 0 0 0 6px rgba(16, 185, 129, 0); }
                100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
            }

            .result-preview {
                background: rgba(16, 185, 129, 0.05);
                border: 1px solid rgba(16, 185, 129, 0.15);
                border-radius: 14px;
                padding: 1rem;
                margin-top: 1rem;
                display: none;
            }
            .result-preview.active {
                display: block;
                animation: fadeIn 0.5s ease;
            }

            @keyframes fadeIn {
                from { opacity: 0; transform: translateY(5px); }
                to { opacity: 1; transform: translateY(0); }
            }
        </style>
    </head>
    <body>
        <header>
            <div>
                <h1>Enigma Medallion Pipeline</h1>
                <div class="subtitle">Simulação local de arquitetura Medallion e streaming de dados</div>
            </div>
            <div style="display: flex; gap: 1rem;">
                <button class="btn-trigger" onclick="runPipeline()">
                    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="5 3 19 12 5 21 5 3"></polygon></svg>
                    Executar Pipeline
                </button>
            </div>
        </header>

        <main>
            <section class="pipeline-flow">
                <div class="flow-card card-producer">
                    <span class="layer-badge badge-producer">Streaming</span>
                    <div class="card-title">1. Producer</div>
                    <div class="card-desc">Simula o streaming enviando pacotes fora de ordem e duplicados.</div>
                </div>
                <div class="flow-card card-consumer">
                    <span class="layer-badge badge-consumer">Queue</span>
                    <div class="card-title">2. Consumer</div>
                    <div class="card-desc">Valida o payload básico e enfileira para ingestão imediata.</div>
                </div>
                <div class="flow-card card-bronze">
                    <span class="layer-badge badge-bronze">Bronze Layer</span>
                    <div class="card-title">3. Raw Landing</div>
                    <div class="card-desc">Armazena os dados brutos exatamente como chegaram. Formato JSON.</div>
                </div>
                <div class="flow-card card-silver">
                    <span class="layer-badge badge-silver">Silver Layer</span>
                    <div class="card-title">4. Clean & Sort</div>
                    <div class="card-desc">Remove duplicados pelo índice, ordena e limpa os campos de texto.</div>
                </div>
                <div class="flow-card card-gold">
                    <span class="layer-badge badge-gold">Gold Layer</span>
                    <div class="card-title">5. Aggregated</div>
                    <div class="card-desc">Junta as partes, valida a integridade sequencial e gera o resultado.</div>
                </div>
                <div class="flow-card card-api">
                    <span class="layer-badge badge-api">Rest API</span>
                    <div class="card-title">6. Integration</div>
                    <div class="card-desc">API final de consumo que recebe a mensagem unificada e consolidada.</div>
                </div>
            </section>

            <div id="result-preview-box" class="result-preview">
                <h4 style="color: var(--color-success); font-size: 1rem; margin-bottom: 0.5rem; display: flex; align-items: center; gap: 0.5rem;">
                    <span class="pulsing-dot"></span> Pipeline executado com sucesso em tempo real!
                </h4>
                <div id="result-text" style="font-family: monospace; font-size: 0.9rem; background: rgba(0,0,0,0.3); padding: 1rem; border-radius: 8px; max-height: 250px; overflow-y: auto; white-space: pre-wrap;"></div>
            </div>

            <section class="dashboard-grid">
                <div class="panel">
                    <div class="panel-title">
                        <span>Produtos de Dados Finais Recebidos na API (Camada Gold)</span>
                        <span style="font-size: 0.8rem; color: var(--text-muted); font-weight: normal;">Atualização em tempo real</span>
                    </div>

                    <table class="run-table">
                        <thead>
                            <tr>
                                <th>Run ID</th>
                                <th>Mensagem Consolidada (Gold final)</th>
                                <th>Partes</th>
                                <th>Integridade</th>
                                <th>Recebido em (API)</th>
                            </tr>
                        </thead>
                        <tbody id="runs-tbody">
                        </tbody>
                    </table>

                    <div id="empty-state" class="empty-state">
                        <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                            <rect x="3" y="3" width="18" height="18" rx="2" ry="2"></rect>
                            <line x1="9" y1="9" x2="15" y2="9"></line>
                            <line x1="9" y1="13" x2="15" y2="13"></line>
                            <line x1="9" y1="17" x2="13" y2="17"></line>
                        </svg>
                        <p>Nenhuma run executada ainda. Use os botões no topo para simular um fluxo de dados!</p>
                    </div>
                </div>
            </section>
        </main>

        <div id="toast" class="toast">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#10b981" stroke-width="2.5"><polyline points="20 6 9 17 4 12"></polyline></svg>
            <span id="toast-message">Pipeline executado com sucesso!</span>
        </div>

        <script>
            async function fetchRuns() {
                try {
                    const response = await fetch('/api/data');
                    const data = await response.json();
                    
                    const tbody = document.getElementById('runs-tbody');
                    const emptyState = document.getElementById('empty-state');
                    
                    if (data.length === 0) {
                        tbody.innerHTML = '';
                        emptyState.style.display = 'block';
                        return;
                    }
                    
                    emptyState.style.display = 'none';
                    tbody.innerHTML = data.map(run => {
                        const date = new Date(run.received_at_api).toLocaleTimeString();
                        const isOk = run.data_integrity_status === 'COMPLETED';
                        
                        return `
                            <tr>
                                <td style="font-weight: bold; font-family: monospace; color: var(--accent-cyan);">${run.pipeline_run_id}</td>
                                <td style="color: #ffffff; font-weight: 600;">"${run.full_payload}"</td>
                                <td>${run.assembled_parts_count} partes (${run.min_index} a ${run.max_index})</td>
                                <td>
                                    <span class="status-tag ${isOk ? 'status-completed' : 'status-incomplete'}">
                                        ${isOk ? '100% Integra' : 'Gaps detectados'}
                                    </span>
                                </td>
                                <td style="color: var(--text-muted);">${date}</td>
                            </tr>
                        `;
                    }).join('');
                } catch (e) {
                    console.error("Erro ao buscar logs da API", e);
                }
            }

            async function runPipeline() {
                try {
                    const response = await fetch(`/api/trigger`, { method: 'POST' });
                    const result = await response.json();
                    
                    const previewBox = document.getElementById('result-preview-box');
                    const resultText = document.getElementById('result-text');
                    
                    previewBox.classList.add('active');
                    resultText.textContent = JSON.stringify(result, null, 4);
                    
                    showToast(`Pipeline executado! Mensagem: "${result.gold.assembled_message}"`);
                    
                    await fetchRuns();
                } catch (e) {
                    console.error("Erro ao rodar pipeline", e);
                    showToast("Erro ao comunicar com a API de trigger", true);
                }
            }

            function showToast(message, isError = false) {
                const toast = document.getElementById('toast');
                const toastMessage = document.getElementById('toast-message');
                toastMessage.textContent = message;
                
                if (isError) {
                    toast.style.borderColor = '#ef4444';
                } else {
                    toast.style.borderColor = 'var(--accent-cyan)';
                }
                
                toast.classList.add('show');
                setTimeout(() => {
                    toast.classList.remove('show');
                }, 4000);
            }

            fetchRuns();
            setInterval(fetchRuns, 5000);
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)
