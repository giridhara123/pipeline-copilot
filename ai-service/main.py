import os
from typing import List
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import anthropic
from fastembed import TextEmbedding

load_dotenv(dotenv_path="../.env")

app = FastAPI(title="PipelineCopilot AI Service")
client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# fastembed uses ONNX (not PyTorch) — starts in 2-3s, 384-dim, no HuggingFace cache conflicts.
_embed_model = TextEmbedding("BAAI/bge-small-en-v1.5")

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
    # fastembed returns a generator — take the first (and only) result.
    vec = list(_embed_model.embed([req.text]))[0].tolist()
    return EmbedResponse(embedding=vec, dimensions=len(vec))


PR_SYSTEM = """You are PipelineCopilot, an expert code reviewer.
You will be given a pull request diff and metadata. Return a JSON object with exactly these fields:
{
  "summary": "2-3 sentence plain-English description of what this PR does",
  "risk_level": one of: "low" | "medium" | "high",
  "risk_flags": ["list of specific risk concerns", "e.g. modifies auth middleware", "no tests added"],
  "checklist": ["list of concrete review questions", "e.g. Are error responses standardized?"]
}
Rules:
- risk_level is "high" if: touches auth/security, removes tests, changes DB schema, modifies CI/CD
- risk_level is "medium" if: touches shared utilities, changes API contracts, large diff (>200 lines)
- risk_level is "low" if: docs, small isolated changes, adds tests
- risk_flags: 2-5 items, each one specific and actionable
- checklist: 2-4 items as yes/no questions a reviewer should verify
Return ONLY the JSON object. No markdown, no explanation, no code fences."""


class PRSummaryRequest(BaseModel):
    pr_number: int
    title: str
    author: str
    repo: str
    base_branch: str
    diff_content: str


class PRSummaryResponse(BaseModel):
    summary: str
    risk_level: str
    risk_flags: List[str]
    checklist: List[str]


@app.post("/summarize-pr", response_model=PRSummaryResponse)
async def summarize_pr(req: PRSummaryRequest):
    # Truncate diff to stay within token budget (~12k chars)
    diff = req.diff_content[-12000:] if len(req.diff_content) > 12000 else req.diff_content

    user_msg = f"""PR #{req.pr_number}: {req.title}
Repo: {req.repo}  |  Author: @{req.author}  |  Base: {req.base_branch}

--- DIFF START ---
{diff}
--- DIFF END ---"""

    try:
        import json
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=PR_SYSTEM,
            messages=[{"role": "user", "content": user_msg}],
        )
        data = json.loads(message.content[0].text)
        return PRSummaryResponse(**data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health():
    return {"status": "ok"}
