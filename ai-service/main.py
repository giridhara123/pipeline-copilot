import os
from typing import List
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import anthropic
from sentence_transformers import SentenceTransformer

load_dotenv(dotenv_path="../.env")

app = FastAPI(title="PipelineCopilot AI Service")
client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# Loaded once at startup. all-MiniLM-L6-v2 is 80MB, 384-dim, runs on CPU in ~10ms.
_embed_model = SentenceTransformer("all-MiniLM-L6-v2")

DIAGNOSE_SYSTEM = """You are PipelineCopilot, an expert CI/CD failure analyst.
You will be given raw CI/CD log output. Analyze it and return a JSON object with exactly these fields:
{
  "summary": "One sentence describing what failed",
  "root_cause": "One sentence explaining WHY it failed",
  "category": one of: "code_defect" | "test_failure" | "flaky_test" | "dependency" | "infra_environment" | "config_secrets" | "timeout_resource",
  "confidence": a number 0.0-1.0 indicating how confident you are,
  "next_step": "One concrete action the developer should take to fix this"
}
Return ONLY the JSON object. No markdown, no explanation, no code fences."""


class DiagnoseRequest(BaseModel):
    run_id: str
    repo: str
    branch: str
    commit_sha: str
    commit_msg: str
    workflow_name: str
    log_content: str


class DiagnoseResponse(BaseModel):
    summary: str
    root_cause: str
    category: str
    confidence: float
    next_step: str


@app.post("/diagnose", response_model=DiagnoseResponse)
async def diagnose(req: DiagnoseRequest):
    # Truncate log to stay within token budget (~12k chars ≈ 3k tokens)
    log = req.log_content[-12000:] if len(req.log_content) > 12000 else req.log_content

    user_msg = f"""Repo: {req.repo}
Branch: {req.branch}
Commit: {req.commit_sha[:7]} — {req.commit_msg}
Workflow: {req.workflow_name}

--- LOG START ---
{log}
--- LOG END ---"""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=512,
            system=DIAGNOSE_SYSTEM,
            messages=[{"role": "user", "content": user_msg}],
        )
        import json
        data = json.loads(message.content[0].text)
        return DiagnoseResponse(**data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class EmbedRequest(BaseModel):
    text: str


class EmbedResponse(BaseModel):
    embedding: List[float]
    dimensions: int


@app.post("/embed", response_model=EmbedResponse)
async def embed(req: EmbedRequest):
    # Semantic embedding via sentence-transformers (all-MiniLM-L6-v2, 384-dim).
    # Unlike hash-based approaches, this understands that "JWT expired" and
    # "token has expired" are the same concept → high cosine similarity.
    vec = _embed_model.encode(req.text, normalize_embeddings=True).tolist()
    return EmbedResponse(embedding=vec, dimensions=len(vec))


@app.get("/health")
async def health():
    return {"status": "ok"}
