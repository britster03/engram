"""Host CLI for isolated provisioning, checkpoints, evaluation and monitoring."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import time
import uuid
from collections import Counter
from pathlib import Path

from . import DATASET_SHA256, SCHEMA_VERSION, UPSTREAM_REVISION
from .dataset import audit, evaluations, load, select
from .infrastructure import ROOT, compose, image_names, prepare
from .ledger import atomic, digest, recover_offline


def output(command):
    return subprocess.check_output(command, cwd=ROOT, text=True).strip()


def verify_runtime_source(hashes):
    """A new benchmark harness must never silently evaluate an old engine image."""
    expected = {name: value for name, value in hashes.items()
                if name.startswith(("engram/", "templates/"))}
    script = ("import engram,pathlib,hashlib,json; root=pathlib.Path(engram.__file__).parent.parent; "
              "expected=" + repr(expected) + "; "
              "bad=[name for name,digest in expected.items() if not (root/name).is_file() "
              "or hashlib.sha256((root/name).read_bytes()).hexdigest()!=digest]; "
              "print(json.dumps(bad))")
    mismatches = json.loads(output(["docker", "run", "--rm", "--network", "none",
                                   image_names()["benchmark"], "python", "-c", script]))
    if mismatches:
        raise RuntimeError("Benchmark image differs from source; build a fresh tag: " + ", ".join(mismatches[:5]))
    return {"status": "VERIFIED", "files": len(expected)}


def manifest(directory, profile, config):
    from .descriptive import REPORT_SCHEMA_VERSION
    from .vendor import verify_official_scorer
    scorer_verification = verify_official_scorer()
    dataset = load()
    selected = select(dataset, profile)
    work = [e for c in dataset for e in evaluations(c, selected)]
    source_files = [p for root in [ROOT/"engram",ROOT/"templates",ROOT/"benchmarks/lifecycle"] for p in root.rglob("*") if p.is_file() and "__pycache__" not in p.parts and p.suffix != ".pyc"]
    hashes = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in source_files}
    import yaml
    configuration = yaml.safe_load((directory / ".runtime/config.yaml").read_text())
    config_hash = digest(configuration)
    value = {"schema_version":SCHEMA_VERSION,"run_id":directory.name,"profile":profile,"seed":42,"git_sha":output(["git","rev-parse","HEAD"]),"dirty_worktree":bool(output(["git","status","--porcelain"])),"working_tree_status":output(["git","status","--porcelain"]).splitlines(),"config_hash":config_hash,"config_source":str(Path(config).resolve()),"source_hashes":hashes,"dataset_sha256":DATASET_SHA256,"official_upstream_revision":UPSTREAM_REVISION,"dataset_audit":audit(dataset),"expected_questions":len(selected),"expected_evaluations":len(work),"expected_tracks":dict(Counter(e.track for e in work)),"hardware":{"platform":platform.platform(),"cpu":platform.processor(),"cpu_count":os.cpu_count()},"cache_policy":"Session and embedding Redis caches cleared on restore; native cache behavior within a frozen block. Cache encoding measured separately.","query_clock":"native real-time; literal dataset dates stored in UTC","comparison_protocol":{"candidates_per_channel":20,"rrf_k":60,"methods":["bm25","vector","hybrid","kg","hybrid_kg"],"answers_generated":False,"ranking":"native heuristics","hybrid_kg_order":"hybrid first then shortest graph bundles, union of shared-claim bundles"},"native_prompts_changed":False,"production_services_modified":False,"images":{}}
    path = directory/"run_manifest.json"
    # Pin the actual isolated images and disclose changed quality policies.
    value["images"] = {name: output(["docker", "image", "inspect", name, "--format", "{{.Id}}"])
                       for name in image_names().values()}
    value["quality_modes"] = {section: configuration.get(section, {}).get(field, "legacy")
                              for section, field in [("retrieval", "quality_mode"),
                                  ("canonical_memory", "evidence_mode"), ("consolidation", "quality_mode")]}
    value["native_prompts_changed"] = "enforce" in value["quality_modes"].values()
    value["compaction_policy"] = configuration.get("session", {})
    from engram.tokens import ENCODING_NAME
    value["compaction_encoding"] = ENCODING_NAME
    value["report_schema_version"] = REPORT_SCHEMA_VERSION
    value["official_scorer_verification"] = scorer_verification
    value["runtime_source"] = verify_runtime_source(hashes)
    if path.exists():
        previous=json.loads(path.read_text())
        if any(previous[key] != value[key] for key in ["profile","git_sha","config_hash","dataset_sha256","source_hashes","images"]):
            raise RuntimeError("Cannot resume with changed source/config/image identity")
        return previous
    atomic(path,value)
    return value


def verify_stack(directory):
    from .infrastructure import dotenv
    env=dotenv(directory/".runtime/compose.env")
    project=env["LOCOMO_PROJECT"]
    ids=output(["docker","ps","-a","--filter",f"label=com.docker.compose.project={project}","--format","{{.ID}}"]).splitlines()
    inspected=json.loads(output(["docker","inspect",*ids]))
    for container in inspected:
        if container["HostConfig"].get("PortBindings"):
            raise RuntimeError("Benchmark service unexpectedly publishes a port")
        if any(not name.startswith(project+"_") for name in container["NetworkSettings"]["Networks"]):
            raise RuntimeError("Benchmark service shares an external network")
        for mount in container["Mounts"]:
            if mount["Type"] == "volume" and not mount["Name"].startswith(project+"_"):
                raise RuntimeError("Benchmark shares an external volume")
    atomic(directory/"stack_verified.json",{"status":"VERIFIED","project":project,"containers":[{"id":c["Id"],"name":c["Name"],"image":c["Image"]} for c in inspected],"published_ports":False,"external_networks":False,"shared_service_volumes":False})


def monitor(directory, action):
    progress=directory/"progress.json"
    if progress.exists():
        state=json.loads(progress.read_text())
        print("BENCHMARK "+json.dumps({k:state.get(k) for k in ["phase","conversation","checkpoint","completed","expected","remaining","errors","known_tokens","known_lifecycle_tokens","unknown_usage_calls","query_unknown_usage_calls","in_flight_calls","elapsed_seconds"]}),flush=True)
    services=compose(directory,"ps","--all","--format","json",capture=True,check=False)
    observed=[json.loads(line) for line in services.stdout.splitlines() if line.strip()]
    required={"postgres","neo4j","redis","temporal","worker","dispatcher","driver"}
    failed=[{"service":s["Service"],"state":s.get("State"),"health":s.get("Health")} for s in observed if s["Service"] in required and (s.get("State") not in {"running","paused"} or s.get("Health")=="unhealthy")]
    missing=required-{s["Service"] for s in observed}
    if services.returncode or failed or missing:
        atomic(directory/"service_failure.json",{"failed":failed,"missing":sorted(missing),"action":action,"inspection_exit_code":services.returncode})
        raise RuntimeError("Benchmark service readiness failed; see service_failure.json")


def rpc(directory, action, **values):
    queue=directory/"commands"
    queue.mkdir(exist_ok=True)
    identity=f"{time.time_ns()}-{uuid.uuid4().hex}"
    path=queue/(identity+".request.json")
    reply=queue/(identity+".response.json")
    atomic(path,{"action":action,**values})
    previous=0
    while not reply.exists():
        time.sleep(2)
        if time.monotonic()-previous >= 30:
            monitor(directory,action)
            previous=time.monotonic()
        result=compose(directory,"ps","--format","json","driver",capture=True,check=False)
        if '"State":"running"' not in result.stdout.replace(" ",""):
            raise RuntimeError("Benchmark driver stopped; retained attempt journals require inspection")
    result=json.loads(reply.read_text())
    if result["status"] != "COMPLETE":
        raise RuntimeError(f"Benchmark action {action} failed: {result.get('error')}")
    return result.get("result")


def validation(directory):
    tests=["tests/benchmarks","tests/unit/test_quality_pipeline.py","tests/unit/test_ordered_retrieval.py","tests/unit/test_hybrid_retrieval.py","tests/unit/test_token_usage.py","tests/unit/test_retrieval_coverage.py","tests/integration/test_ordered_session_retrieval.py","tests/integration/test_ordered_graph_retrieval.py","tests/integration/test_overview_lineage.py","tests/integration/test_quality_retrieval.py","tests/integration/test_provider_recovery.py"]
    result=compose(directory,"run","--rm","--no-deps","-e","LOCOMO_TRACE_DIR=/tmp/lifecycle-validation","driver","python","-m","pytest",*tests,"-q","-p","no:cacheprovider",capture=True,check=False)
    (directory/"validation.log").write_text(result.stdout+result.stderr)
    if result.returncode:
        raise RuntimeError("Benchmark instrumentation/integration validation failed; see validation.log")
    atomic(directory/"validation.json",{"status":"PASSED","output":result.stdout,"tests":tests})


def _run(profile, directory, config, *, skip_validation=False):
    directory=Path(directory).resolve()
    directory.mkdir(parents=True,exist_ok=True)
    project="engram-lifecycle-"+profile+"-"+hashlib.sha256(str(directory).encode()).hexdigest()[:12]
    prepare(directory,config,project)
    meta=manifest(directory,profile,config)
    service_state=compose(directory,"ps","--all","--format","json",capture=True,check=False)
    services=[json.loads(line) for line in service_state.stdout.splitlines() if line.strip()]
    if not any(s["Service"] in {"worker","dispatcher","driver"} and s.get("State") in {"running","paused"} for s in services):
        recover_offline(directory)
    print(f"Starting {profile}: {meta['expected_questions']} questions; {meta['expected_tracks']}",flush=True)
    # Native source image and benchmark tools use separate tags. No production
    # container is selected by any Compose command in this module.
    compose(directory,"up","-d","worker","dispatcher","driver")
    ready=directory/"commands/driver_ready.json"
    deadline=time.monotonic()+180
    while not ready.exists():
        if time.monotonic()>deadline:
            raise RuntimeError("Driver readiness timeout")
        time.sleep(2)
    verify_stack(directory)
    if not skip_validation:
        validation(directory)
    dataset=load()
    selected=select(dataset,profile)
    failures=[]
    try:
        for conversation in dataset:
            if not any(q.qa_id in selected for q in conversation.questions):
                continue
            work=evaluations(conversation,selected)
            cuts={e.cutoff for e in work if e.variant in {"evidence_session","following_session"}}
            completed_path=directory/"checkpoints"/conversation.id/"completed.json.gz"
            try:
                if not completed_path.exists():
                    resume=rpc(directory,"resume_info",conversation=conversation.id)
                    for session in conversation.sessions:
                        if session in resume["committed_sessions"]:
                            continue
                        compose(directory,"unpause","worker","dispatcher",check=False,capture=True)
                        rpc(directory,"ingest",conversation=conversation.id,session=session)
                        rpc(directory,"wait",conversation=conversation.id,session=session)
                        compose(directory,"pause","worker","dispatcher")
                        rpc(directory,"snapshot",conversation=conversation.id,session=session,final=False)
                        if session in cuts:
                            # Evaluate this verified active-session checkpoint
                            # immediately. No QA enters history; native writes
                            # resume only after unchanged-state verification.
                            rpc(directory,"evaluate",conversation=conversation.id,session=session,final=False,profile=profile)
                        compose(directory,"unpause","worker","dispatcher")
                        rpc(directory,"close",conversation=conversation.id,session=session)
                        rpc(directory,"wait",conversation=conversation.id,session=session)
                    rpc(directory,"wait",conversation=conversation.id,session=conversation.sessions[-1])
                    compose(directory,"pause","worker","dispatcher")
                    rpc(directory,"snapshot",conversation=conversation.id,session=conversation.sessions[-1],final=True)
                else:
                    compose(directory,"pause","worker","dispatcher",check=False,capture=True)
                for cutoff in sorted(cuts):
                    rpc(directory,"evaluate",conversation=conversation.id,session=cutoff,final=False,profile=profile)
                rpc(directory,"evaluate",conversation=conversation.id,session=conversation.sessions[-1],final=True,profile=profile)
            except Exception as error:
                failures.append({"conversation":conversation.id,"error_type":type(error).__name__,"message":str(error)})
                atomic(directory/"block_failures.json",failures)
                print("INCOMPLETE block: "+json.dumps(failures[-1]),flush=True)
        rpc(directory,"report",provisional=True)
        rpc(directory,"phase",phase="secondary_scoring")
        from .infrastructure import dotenv
        scoring_name=dotenv(directory/".runtime/compose.env")["LOCOMO_PROJECT"]+"-secondary-scoring"
        try:
            result=compose(directory,"run","--rm","--no-deps","--name",scoring_name,"scoring",capture=True,check=False,monitor=lambda:monitor(directory,"secondary_scoring"))
        finally:
            # A failed monitoring callback must not orphan the private CPU job.
            subprocess.run(["docker","rm","--force",scoring_name],capture_output=True,check=False)
        (directory/"secondary_scoring.log").write_text(result.stdout+result.stderr)
        rpc(directory,"phase",phase="reporting")
        summary=rpc(directory,"report",provisional=False)
        atomic(directory/"rollout_status.json",{"summary":summary,"block_failures":failures,"secondary_exit_code":result.returncode})
        rpc(directory,"phase",phase="complete" if summary["status"] in {"COMPLETE","COMPLETE_WITH_ERRORS"} else "incomplete",status=summary["status"])
        return summary
    finally:
        # Stop only isolated writers/driver; preserve data and diagnostics.
        compose(directory,"kill","-s","SIGKILL","worker","dispatcher",check=False,capture=True)
        compose(directory,"stop","driver",check=False,capture=True)


def run(profile,directory,config,*,skip_validation=False):
    import fcntl
    directory=Path(directory).resolve()
    directory.mkdir(parents=True,exist_ok=True)
    with (directory/"host.lock").open("a") as lock:
        try:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("A benchmark host already owns this run directory") from None
        return _run(profile,directory,config,skip_validation=skip_validation)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile",choices=["pilot","validation","full","pilot-then-full"],default="pilot-then-full")
    parser.add_argument("--config",default="config.prod.yaml")
    parser.add_argument("--run-dir",type=Path)
    parser.add_argument("--build",action="store_true")
    parser.add_argument("--image-tag", default=os.environ.get("LOCOMO_IMAGE_TAG", "20260930"),
                        help="Use a fresh tag for changed source; retain earlier benchmark images")
    parser.add_argument("--skip-validation",action="store_true",help="For development only; run is not publishable without validation.json")
    args=parser.parse_args()
    os.environ["LOCOMO_IMAGE_TAG"] = args.image_tag
    if args.build:
        images = image_names()
        subprocess.run(["docker","build","-t",images["native"],"-f","Dockerfile","."],cwd=ROOT,check=True)
        for target in ("benchmark", "scoring"):
            subprocess.run(["docker","build","--target",target,"-t",images[target],"--build-arg", "BASE_IMAGE=" + images["native"],"-f","benchmarks/lifecycle/Dockerfile","benchmarks"],cwd=ROOT,check=True)
    profiles=["pilot","validation","full"] if args.profile == "pilot-then-full" else [args.profile]
    group=args.run_dir or ROOT/"benchmarks/results/lifecycle"/time.strftime("%Y%m%d-%H%M%S")
    for profile in profiles:
        directory=group/profile if len(profiles)>1 else group
        result=run(profile,directory,args.config,skip_validation=args.skip_validation)
        print(json.dumps(result),flush=True)
        if result["status"] not in {"COMPLETE","COMPLETE_WITH_ERRORS"}:
            print("Rollout stopped: incomplete pilot/validation measurements require inspection.",flush=True)
            raise SystemExit(2)
        # Release phase RAM while retaining private databases, volumes, snapshots
        # and reports for later inspection or resume. No production project is used.
        compose(directory,"stop","postgres","neo4j","redis","temporal",capture=True)


if __name__ == "__main__":
    main()
