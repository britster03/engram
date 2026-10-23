import ast
import csv
import contextlib
import io
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from openai.types.responses import Response

from benchmarks.lifecycle import dataset
from benchmarks.lifecycle.ledger import atomic, append, read, safe, digest
from benchmarks.lifecycle.metrics import evidence_metrics, compatible, bootstrap_conversations
from benchmarks.lifecycle.observer import Trace, capture, instrument_provider, summarize_usage
from benchmarks.lifecycle.scoring import score
from benchmarks.lifecycle.report import csv_write, accounting, denominator_rows


@pytest.fixture(scope="module")
def data():
    return dataset.load()


@pytest.fixture(autouse=True)
def isolate_mock_provider_journals(tmp_path,monkeypatch):
    monkeypatch.setenv("LOCOMO_TRACE_DIR",str(tmp_path))


def test_dataset_fidelity_and_tracks(data):
    assert dataset.audit(data) == {"conversations":10,"sessions":272,"turns":5882,"captions":1226,"questions":1986,"valid_positive_questions":1533,"categories":{"multi_hop":282,"temporal":321,"open_domain":96,"single_hop":841,"adversarial":446}}
    work=[e for c in data for e in dataset.evaluations(c,dataset.select(data,"full"))]
    assert sum(e.track == "CANONICAL" for e in work)==1986
    assert sum(e.track == "SESSION_ROUTING" for e in work)==3471
    assert sum(e.track == "SEARCH_COMPARISON" for e in work)==9930
    assert len({e.key for e in work})==len(work)


@pytest.mark.parametrize("profile,n",[("pilot",25),("validation",50),("full",1986)])
def test_deterministic_selection(data,profile,n):
    assert len(dataset.select(data,profile))==n
    assert dataset.select(data,profile)==dataset.select(data,profile,42)


def test_dates_captions_no_annotation_leak(data):
    first=data[0].turns[0]
    assert first.timestamp.endswith("+00:00")
    assert first.speaker in first.content and first.raw_timestamp in first.content
    caption=next(t for c in data for t in c.turns if t.caption)
    assert caption.caption in caption.content
    assert "qa_id" not in first.content and "dia_id" not in first.content


@pytest.mark.parametrize("raw,expected,status",[( ["D:1:2"],("D1:2",),"NORMALIZED"),(["D1:2 D1:3"],("D1:2","D1:3"),"NORMALIZED"),(["D1:2","D1:2"],("D1:2",),"NORMALIZED"),(["D99:2"],("D99:2",),"INVALID"),(["D"],(),"INVALID"),([],(),"MISSING")])
def test_annotation_normalization(raw,expected,status):
    ids,result,_=dataset.normalize_evidence(raw,{"D1:2","D1:3"})
    assert (ids,result)==(expected,status)


def test_checkpoint_annotations_are_scoped(data):
    for conv in data:
        for e in dataset.evaluations(conv,dataset.select(data,"full")):
            if e.variant in {"evidence_session","following_session"}:
                assert max(e.question.evidence_sessions)<=e.cutoff
            if e.variant == "following_session":
                assert e.expected_level=="L2"
    assert sum(q.annotation_status == "INVALID" for c in data for q in c.questions)==3


@pytest.mark.parametrize("cat,pred,gold,value",[(1,"red, blue","red, blue",1),(1,"red","red, blue",.5),(2,"running","run",1),(3,"Paris","Paris; France",1),(4,"The school","school",1),(5,"Not mentioned in the dialogue.",None,1),(5,"I cannot determine that.",None,0),(5,"no information available",None,1)])
def test_official_scores(cat,pred,gold,value):
    assert score(pred,gold,cat)["official_locomo_f1"]==pytest.approx(value)


def test_official_functions_are_unchanged():
    base=Path("benchmarks/lifecycle/vendor")
    original=ast.parse((base/"evaluation.py.reference").read_text())
    vendored=ast.parse((base/"official_qa.py").read_text())
    expected={n.name:ast.dump(n) for n in original.body if isinstance(n,ast.FunctionDef)}
    assert all(ast.dump(n)==expected[n.name] for n in vendored.body if isinstance(n,ast.FunctionDef))


def test_operational_failure_not_correct_abstention():
    assert score("not mentioned",None,5,operational_status="ERROR")["official_locomo_f1"]==0


def test_multi_hop_recall_and_packing_loss():
    a=evidence_metrics(["a","b"],["x","a","a","b"])
    b=evidence_metrics(["a","b"],["a"])
    assert a["recall_at_1"]==0 and a["recall_at_3"]==1 and a["mrr"]==.5
    assert b["evidence_fraction"]==.5 and not b["all_evidence_recalled"]
    assert b["precision_at_10"]==.1
    assert evidence_metrics(["a"],["a"],evaluable=False)["recall_at_10"] is None


def test_compatible_not_classifier_accuracy():
    assert compatible("L1+L2",["L1","L2"],["a"],[])
    assert compatible("L1+L2",["L3_FACTS"],["a"],["a"])
    assert not compatible("L1+L2",["L1"],["a"],[])
    assert compatible("INVALID",[],[],[]) is None


def response(text, incoming=10, outgoing=5):
    return Response.model_validate({"id":"resp_test","created_at":1,"model":"test","object":"response","status":"completed","error":None,"incomplete_details":None,"instructions":None,"metadata":{},"output":[{"id":"msg_1","role":"assistant","status":"completed","type":"message","content":[{"type":"output_text","text":text,"annotations":[]}]}],"parallel_tool_calls":False,"temperature":1,"tool_choice":"auto","tools":[],"top_p":1,"usage":{"input_tokens":incoming,"output_tokens":outgoing,"total_tokens":incoming+outgoing,"input_tokens_details":{"cached_tokens":2,"cache_write_tokens":0},"output_tokens_details":{"reasoning_tokens":1}}})


def provider(responses):
    from engram.config import CoreModelConfig
    from engram.models.providers import build_core_provider
    p=build_core_provider(CoreModelConfig(api_key="secret",api_base="https://example.invalid/v1",model_path="test"))
    p._client.responses.create=Mock(side_effect=responses)
    return instrument_provider(p)


def test_usage_includes_json_correction_before_parsing(tmp_path,monkeypatch):
    monkeypatch.setenv("LOCOMO_TRACE_DIR",str(tmp_path))
    p=provider([response("invalid",10,5),response('{"store":true}',20,7)])
    with capture(Trace("parse",component="query")) as trace:
        p.complete(system_prompt="Return JSON",user_prompt="fact")
    usage=summarize_usage(trace.calls)
    assert usage["total_tokens"]==42 and usage["provider_attempts"]==2 and usage["retry_count"]==1
    assert usage["cached_input_tokens"]==4 and usage["total_reasoning_tokens"]==2
    assert len(read(next(tmp_path.glob("provider_attempts.*.jsonl"))))==4
    assert "secret" not in next(tmp_path.glob("provider_attempts.*.jsonl")).read_text()


def test_failed_parse_still_has_returned_usage():
    p=provider([response("bad"),response("still bad")])
    with capture(Trace("bad")) as trace:
        with pytest.raises(Exception):
            p.complete(system_prompt="JSON",user_prompt="test")
    assert summarize_usage(trace.calls)["total_tokens"]==30
    assert len(trace.calls)==2


def test_unknown_billable_failure_keeps_known_subtotal():
    calls=[{"operation_id":"one","input_tokens":10,"output_tokens":5,"reasoning_tokens":1,"cached_input_tokens":2,"token_count_source":"provider"},{"operation_id":"one","input_tokens":None,"output_tokens":None,"token_count_source":"unavailable"}]
    total=summarize_usage(calls)
    assert total["total_tokens"] is None and total["known_total_tokens"]==15
    assert total["unknown_usage_calls"]==1 and total["retry_count"]==1


def test_empty_and_subset_usage():
    assert summarize_usage([])["total_tokens"]==0
    usage=summarize_usage([{"operation_id":"one","phase":"generation","input_tokens":10,"output_tokens":5,"reasoning_tokens":3,"cached_input_tokens":4,"token_count_source":"provider"}])
    assert usage["total_tokens"]==15
    assert usage["generation_output_tokens"]==5
    assert usage["query_rewrite_input_tokens"]==0


def test_csv_serialization_and_atomic_progress(tmp_path):
    csv_write(tmp_path/"data.csv",[{"question":"one,\ntwo","ids":["a","b"],"value":None}])
    import csv
    row=next(csv.DictReader((tmp_path/"data.csv").open()))
    assert row["question"]=="one,\ntwo" and json.loads(row["ids"])==["a","b"] and row["value"]==""
    atomic(tmp_path/"progress.json",{"completed":2})
    assert json.loads((tmp_path/"progress.json").read_text())["completed"]==2
    assert not list(tmp_path.glob("*.tmp"))


def test_ingestion_report_reads_canonical_artifacts_and_keeps_missing_counts(tmp_path):
    import gzip
    from benchmarks.lifecycle.report import ingestion_rows,extraction_counts
    event={"event_id":"accepted","status":"COMPLETE","payload":{"turn_pair":{"user":{"content":"Alice likes running"},"assistant":{"content":""}}},"retry_count":0,"created_at":"2023-01-01T00:00:00+00:00","processed_at":"2023-01-01T00:00:01+00:00"}
    artifacts=[{"event_id":"accepted","artifact_type":"EXTRACTION","artifact_key":key,"payload":value} for key,value in [("gate-v1",{"store":True}),("extract-v1",{"triplets":[{},{}]}),("typed-v1",{"triplets":[{}]})]]
    append(tmp_path/"provenance.jsonl",{"event_id":"accepted","conversation_id":"conv-test","content_sha256":digest("Alice likes running")})
    path=tmp_path/"checkpoints/conv-test/session-001.json.gz"
    path.parent.mkdir(parents=True)
    with gzip.open(path,"wt") as stream:
        json.dump({"postgres":{"events":[event],"ingest_artifacts":artifacts,"extractions":[],"memory_nodes":[{"id":"node","origin_event_id":"accepted"}]}},stream)
    atomic(path.with_suffix(".manifest.json"),{"conversation_id":"conv-test","cutoff":1,"final":False})
    row=ingestion_rows(tmp_path)[0]
    assert row["extracted_triplets"]==2 and row["normalized_extracted_triplets"]==1
    assert row["new_memory_count"]==1 and row["gate_store"]
    assert row["raw_retention_verified"] and row["extraction_count_source"]=="CANONICAL_ARTIFACT"
    assert extraction_counts(event,{},{})["extracted_triplets"] is None
    assert extraction_counts({**event,"status":"GATED_SKIP"},{},{})["extracted_triplets"]==0


def test_actual_model_context_suffix_preserves_only_retained_turns():
    from benchmarks.lifecycle.adapter import Provenance,session_id
    from benchmarks.lifecycle.evaluation import context_evidence
    from engram.session.manager import SessionState,Turn
    utterances=(dataset.Turn("D1:1",1,0,"Alice","Earlier fact","","2023-01-01T00:00:00+00:00","date"),dataset.Turn("D1:2",1,1,"Alice","Later fact","","2023-01-01T00:00:00+00:00","date"))
    conv=dataset.Conversation("conv-test",utterances,())
    sid=session_id(conv.id,1)
    state=SessionState(sid,turns=[Turn("user",t.content,t.timestamp,t.index*2) for t in utterances])
    provenance=Provenance({"postgres":{"session_states":[{"session_id":sid,"payload":state.to_payload()}]}},[],conv)
    original=state.render(max_turns=50)
    retained="USER: "+utterances[-1].content
    msc="## Session context (not canonical memory)\n[… truncated to 20 tokens …]\n"+retained+"\n\n## Retrieval assessment\nanswerable"
    trace=Trace("suffix")
    trace.emit("tool",level="L1",result={"text":original,"sources":[{"session_id":sid}]})
    trace.emit("context",arguments={"session_context":original},result=msc)
    trace.emit("model_context",text=msc)
    info=context_evidence(provenance,trace,{})
    assert info["retrieved_ids"]==["D1:1","D1:2"]
    assert info["packed_evidence_ids"]==["D1:2"] and info["packed_provenance_status"]=="EXACT"
    assert info["supplied_context_levels"]==["L1"]
    tail=provenance.context_tail({"text":original,"sources":[{"session_id":sid}]},original.index("Earlier fact")+2)
    assert tail[0]==["D1:2"] and not tail[1]


def test_partial_original_record_retains_only_whole_later_sources(data):
    from benchmarks.lifecycle.adapter import Provenance
    provenance=Provenance({"postgres":{}},[{"conversation_id":data[0].id,"event_id":e,"dia_id":d} for e,d in [("a","D1:1"),("b","D1:2")]],data[0])
    lines=[json.dumps({"event_id":e,"messages":[{"content":text}]}) for e,text in [("a","earlier"),("b","later")]]
    context={"text":"\n".join(lines),"sources":[{"event_id":"a"},{"event_id":"b"}]}
    assert provenance.context_tail(context,len(lines[0])+1)==(["D1:2"],True,1)
    assert provenance.context_tail(context,2)==(["D1:2"],False,2)


def test_unknown_prefix_positions_cannot_claim_ranked_lower_bounds():
    from benchmarks.lifecycle.report import conservative_lower_bounds
    lower=evidence_metrics(["gold"],["gold"])
    row=conservative_lower_bounds({"provenance_status":"INCOMPLETE","packed_provenance_status":"EXACT","retrieval_lower_bound":lower,"packed_lower_bound":lower.copy()})
    # A preceding summary may contain >20 unknown dialogue positions.
    # Known membership still establishes coverage, but not a rank cutoff.
    assert row["retrieval_lower_bound"]["evidence_fraction"]==1
    assert row["retrieval_lower_bound"]["all_evidence_recalled"] is True
    assert row["retrieval_lower_bound"]["recall_at_20"] is None
    assert row["retrieval_lower_bound"]["mrr"] is None
    assert row["packed_lower_bound"]["recall_at_1"]==1


def test_unmappable_rendered_suffix_is_not_exact_zero_evidence():
    from benchmarks.lifecycle.evaluation import context_evidence
    trace=Trace("unmappable")
    original="USER: Before \U000130c0 after"
    msc="## Session context (not canonical memory)\n[… truncated to 3 tokens …]\n\ufffd after"
    trace.emit("tool",level="L1",result={"text":original})
    trace.emit("context",arguments={"session_context":original},result=msc)
    trace.emit("model_context",phase="generation",text=msc)
    provenance=SimpleNamespace(context=lambda context:(["D1:1"],True,1))
    result=context_evidence(provenance,trace,{})
    assert result["provenance_status"]=="EXACT"
    assert result["packed_evidence_ids"]==[] and result["packed_provenance_status"]=="INCOMPLETE"


def test_failed_sufficiency_context_is_not_final_answer_context(data):
    from benchmarks.lifecycle.adapter import Provenance
    from benchmarks.lifecycle.evaluation import context_evidence
    provenance=Provenance({"postgres":{}},[{"conversation_id":data[0].id,"event_id":"a","dia_id":"D1:1"}],data[0])
    trace=Trace("deterministic")
    trace.emit("tool",level="L1",result={"text":"earlier question context","sources":[{"event_id":"a"}]})
    trace.emit("model_context",text="earlier question context",phase="answerability")
    trace.emit("model_context",text="earlier question context",phase="generation")
    trace.emit("provider_result",phase="generation",result={"verdict":"NEED_MORE"})
    trace.emit("packing",result=[])
    result=context_evidence(provenance,trace,{"answer_mode":"deterministic-canonical"})
    assert result["retrieved_ids"]==["D1:1"] and result["packed_evidence_ids"]==[]
    assert result["supplied_context_levels"]==[] and result["packed_provenance_status"]=="UNAVAILABLE_NO_GENERATION_CONTEXT"


def test_accounting_interrupted_attempt(tmp_path):
    append(tmp_path/"provider_attempts.1.jsonl",{"attempt_id":"a","state":"STARTED","trace_id":"t","component":"query","operation_id":"o"})
    result=accounting(tmp_path)
    assert result["status"]=="IN_FLIGHT_OR_INTERRUPTED"
    assert result["components"]["query"]["unknown_usage_calls"]==1


def test_failed_questions_in_complete_denominator(data):
    result=denominator_rows({"run_id":"r","profile":"pilot","git_sha":"s","config_hash":"c"},[])
    assert sum(r["benchmark_track"]=="CANONICAL" for r in result)==25
    assert all(r["status"]=="NOT_EXECUTED" for r in result)
    assert all(r["official_locomo_f1"]==0 for r in result if r["benchmark_track"]!="SEARCH_COMPARISON")


def test_secret_redaction():
    assert safe({"api_key":"s","admin_key":"s","dsn":"s","request":{"extra_headers":{"Authorization":"s"}},"model":"ok"})=={"request":{},"model":"ok"}


def test_large_trace_archive_is_lossless_linked_and_verified(tmp_path):
    from benchmarks.lifecycle.ledger import append_trace,expand_trace
    trace=Trace("large")
    trace.emit("store",result={"dialogue":"Repeated dialogue \u0939\u093f\u0902\u0926\u0940 "*40000,"scores":[.1,.2]})
    trace.emit("embedding",count=1,latency_ms=2)
    trace.calls.append({"attempt_id":"sdk","input_tokens":12})
    snapshot=trace.to_dict()
    trace.to_dict=lambda:snapshot
    append_trace(tmp_path,trace)
    index=read(tmp_path/"trace.jsonl")[0]
    assert expand_trace(tmp_path,index)==snapshot
    assert index["calls"]==snapshot["calls"] and index["event_count"]==2
    assert index["trace_archive_bytes"]<index["trace_archive_uncompressed_bytes"]/10
    assert len(index["events"])==1 and index["events"][0]["kind"]=="embedding"
    assert expand_trace(tmp_path,{"trace_id":"legacy"})=={"trace_id":"legacy"}
    with pytest.raises(ValueError,match="checksum mismatch"):
        expand_trace(tmp_path,{**index,"trace_archive_sha256":"bad"})
    with pytest.raises(ValueError,match="outside"):
        expand_trace(tmp_path,{**index,"trace_archive":"../other.json.gz"})
    assert accounting(tmp_path,verify_archives=True)["status"]=="RECONCILED"
    (tmp_path/index["trace_archive"]).write_bytes(b"corrupt")
    result=accounting(tmp_path,verify_archives=True)
    assert result["status"]=="MISMATCH" and result["trace_archive_failures"][0]["trace_id"]=="large"


def test_driver_waits_for_private_database_initialization():
    import yaml
    from benchmarks.lifecycle.infrastructure import ROOT
    definition=yaml.safe_load((ROOT/"benchmarks/lifecycle/compose.yaml").read_text())
    for service in ["worker","dispatcher","driver"]:
        assert definition["services"][service]["depends_on"]["init"]["condition"]=="service_completed_successfully"


def test_conversation_bootstrap():
    rows=[{"conversation_id":"a","score":.5},{"conversation_id":"b","score":1}]
    a,b=bootstrap_conversations(rows,"score",samples=200)
    assert .5<=a<=b<=1


def test_no_all_entity_mentions_credit(data):
    from benchmarks.lifecycle.adapter import Provenance
    conv=data[0]
    p=Provenance({"postgres":{"memory_claims":[{"id":"c","source_event_id":"e"}],"memory_nodes":[{"id":"m","current_version_id":None}]}},[{"conversation_id":conv.id,"event_id":"e","dia_id":"D1:1"}],conv)
    assert p.row({"claim_id":"c"})==(["D1:1"],True)
    assert p.row({"memory_id":"m","overview":"entity summary"})==([],False)


def test_ingestion_passes_native_session_tail_without_qa(data,tmp_path,monkeypatch):
    from benchmarks.lifecycle import adapter
    import engram.api.app as app_module
    conversation=data[0]
    source=dataset.Conversation(conversation.id,conversation.turns[:2],())
    requests=[]
    def accept(req):
        requests.append(req)
        return SimpleNamespace(body=json.dumps({"event_id":f"event-{len(requests)}"}).encode())
    monkeypatch.setattr(app_module,"ingest",accept)
    monkeypatch.setattr(adapter,"use_tenant",lambda *args:"locomo-test")
    manager=SimpleNamespace(_persist=lambda *args:None,needs_compaction=lambda *args:False)
    monkeypatch.setattr(adapter,"manager_for",lambda *args:manager)
    state=SimpleNamespace(control_plane=SimpleNamespace(get_session_state=lambda *args,**kwargs:None))
    progress=SimpleNamespace(state={},update=lambda **kwargs:None)
    adapter.ingest_session(state,source,source.sessions[0],tmp_path,progress)
    assert len(requests)==2
    assert requests[0].session_context=="USER: "+source.turns[0].content
    assert source.turns[0].content in requests[1].session_context
    assert source.turns[1].content in requests[1].session_context
    assert all(not r.turn_pair.assistant.content for r in requests)
    assert all(r.session_summary is None for r in requests)
    accepted={r["event_id"]:{"event_id":r["event_id"],"session_id":r["session_id"],"payload":{"turn_pair":{"user":{"content":t.content}}}} for r,t in zip(read(tmp_path/"provenance.jsonl"),source.turns)}
    state.control_plane.get_event=lambda event_id,**kwargs:accepted.get(event_id)
    adapter.ingest_session(state,source,source.sessions[0],tmp_path,progress)
    assert len(requests)==2
    assert len(read(tmp_path/"provenance.jsonl"))==2
    from engram.session.manager import SessionState,Turn
    tail=SessionState(adapter.session_id(source.id,source.sessions[0]),turns=[Turn("user",source.turns[1].content,source.turns[1].timestamp,2)],compacted="Earlier utterances compacted")
    state.control_plane.get_session_state=lambda *args,**kwargs:tail.to_payload()
    manager._persist=Mock()
    resumed=adapter.ingest_session(state,source,source.sessions[0],tmp_path,progress)
    assert resumed.to_payload()==tail.to_payload()
    manager._persist.assert_not_called()
    assert len(requests)==2


def test_resume_uses_native_committed_prefix(data,tmp_path,monkeypatch):
    from benchmarks.lifecycle import runner
    monkeypatch.setattr(runner,"use_tenant",lambda *args:"locomo-test")
    states={"locomo-conv-26-s001":{"status":"COMMITTED"},"locomo-conv-26-s002":{"status":"ACTIVE"}}
    state=SimpleNamespace(control_plane=SimpleNamespace(get_session_state=lambda sid,**kwargs:states.get(sid)))
    result=runner.action(state,{"action":"resume_info","conversation":"conv-26"},tmp_path,None)
    assert result=={"latest_session":2,"committed_sessions":[1]}
    states["locomo-conv-26-s001"]["status"]="ACTIVE"
    with pytest.raises(RuntimeError,match="Uncommitted older"):
        runner.action(state,{"action":"resume_info","conversation":"conv-26"},tmp_path,None)


def test_fusion_participation_excludes_timed_out_channel():
    from benchmarks.lifecycle.evaluation import fusion_participation
    trace=Trace("search")
    trace.emit("fusion",inputs=[{"bm25":[{"memory_id":"a"},{"memory_id":"rejected"}]}],result={"a":.01})
    trace.emit("store",function="vector_search",result=[{"memory_id":"a"}])
    assert fusion_participation(trace)=={"bm25":["a"],"vector":[]}
    trace.emit("fusion",inputs=[{"vector":[{"memory_id":"b"}]}],result={"b":.01})
    assert fusion_participation(trace)=={"bm25":["a"],"vector":["b"]}


def test_semantic_audit_uses_actual_evidence_links_and_entity_names():
    from benchmarks.lifecycle.audit import sourced_artifacts
    pg={"memory_nodes":[{"id":"s","canonical_name":"Alice"},{"id":"o","canonical_name":"Bob"}],"memory_claims":[{"id":"c","subject_id":"s","object_entity_id":"o","predicate":"knows","source_event_id":"older"},{"id":"unrelated","source_event_id":"other"}],"memory_evidence":[{"claim_id":"c","source_event_id":"gold"}]}
    artifacts=sourced_artifacts(pg,{"gold"})
    assert len(artifacts)==1
    assert artifacts[0]["subject_name"]=="Alice" and artifacts[0]["object_entity_name"]=="Bob"
    assert artifacts[0]["supporting_event_ids"]==["gold"]


def test_verified_paths_require_the_actual_whole_bundle():
    from benchmarks.lifecycle.evaluation import path_bundle
    assert path_bundle({"claims":[{"claim_id":"b"},{"claim_id":"a"}]})=="path:a:b"
    assert path_bundle({"claims":[{}]}) is None
    assert path_bundle({"claims":[{"claim_id":"a"},{"claim_id":"b"}]}) not in {"path:a","path:b"}


def test_paired_search_confidence_intervals_keep_denominators():
    from benchmarks.lifecycle.report import paired_aggregates
    rows=[{"benchmark_track":"SEARCH_COMPARISON","qa_id":"qa1","conversation_id":"c1","search_method":method,"evidence_recall_at_k":{"10":value}} for method,value in [("bm25",0),("hybrid",1),("vector",None)]]
    result={r["method"]:r for r in paired_aggregates(rows,bootstrap_samples=100)}
    assert result["hybrid"]["recall_at_10_delta"]==1
    # One independent conversation cannot establish between-conversation uncertainty.
    assert result["hybrid"]["ci_low"] is None and result["hybrid"]["ci_high"] is None
    assert result["vector"]["expected_pairs"]==1 and result["vector"]["evaluable_pairs"]==0


def test_full_coverage_with_query_failures_is_visible_without_blocking_rollout(tmp_path,data):
    from benchmarks.lifecycle.report import generate
    manifest={"run_id":"test","profile":"pilot","git_sha":"test","config_hash":"test"}
    atomic(tmp_path/"run_manifest.json",manifest)
    atomic(tmp_path/"validation.json",{"status":"PASSED"})
    atomic(tmp_path/"stack_verified.json",{"status":"VERIFIED"})
    rows=denominator_rows(manifest,[])
    for row in rows:
        row.update(status="ERROR",semantic_preservation_status="AUDIT_ERROR",secondary_metric_status="OPERATIONAL_FAILURE",trace_id=row["evaluation_id"],total_latency_ms=1,routing_latency_ms=0,total_tokens=0)
        append(tmp_path/"result_rows.jsonl",row)
        append(tmp_path/"trace.jsonl",{"trace_id":row["trace_id"],"calls":[]})
        atomic(tmp_path/"verified_blocks"/f"{row['conversation_id']}-{row['evaluation_cutoff_session']}-{row['checkpoint_variant'] in {'completed','annotation_limited'}}.json",{"status":"VERIFIED","before":{"p":"same"},"after":{"p":"same"}})
    result=generate(tmp_path,provisional=False)
    assert result["status"]=="COMPLETE_WITH_ERRORS" and result["executed"]==193
    assert result["question_errors"]==193
    assert "Category results" in (tmp_path/"summary.html").read_text()
    assert "CANONICAL: 0/25" in (tmp_path/"summary.html").read_text()
    assert "Recall@10 evaluable rows" in (tmp_path/"summary.md").read_text()
    with (tmp_path/"management.csv").open(newline="") as stream:
        management=list(csv.DictReader(stream))
    assert len(management)==25
    assert all("Retrieved Data / Frontier Context" in row for row in management)
    assert json.loads(management[0]["Retrieved Data / Frontier Context"])["frontier_calls"]==[]
    proof=next((tmp_path/"verified_blocks").glob("*.json"))
    atomic(proof,{"status":"VERIFIED","before":{"p":"before"},"after":{"p":"after"}})
    assert generate(tmp_path,provisional=False)["status"]=="INCOMPLETE"


def test_scoring_outputs_belong_to_host_run_owner(tmp_path,monkeypatch):
    from benchmarks.lifecycle.scoring import restore_output_ownership
    import os
    monkeypatch.setattr(os,"geteuid",lambda:0)
    chown=Mock()
    monkeypatch.setattr(os,"chown",chown)
    path=tmp_path/"local_scoring_attempts.jsonl"
    path.write_text("{}");(tmp_path/"secondary_scores.json").symlink_to(path)
    restore_output_ownership(tmp_path)
    chown.assert_called_once_with(path,tmp_path.stat().st_uid,tmp_path.stat().st_gid)


def test_local_encoder_accounting_counts_real_positions_separately(tmp_path):
    import torch
    from benchmarks.lifecycle.scoring import observe_local_encoder
    from benchmarks.lifecycle.report import local_scoring_accounting
    model=SimpleNamespace(forward=lambda ids,**kwargs:ids)
    observe_local_encoder(model,tmp_path,["q1","q2"])
    ids=torch.tensor([[1,2,3],[4,0,0]])
    assert torch.equal(model.forward(ids,attention_mask=torch.tensor([[1,1,1],[1,0,0]])),ids)
    result=local_scoring_accounting(tmp_path)
    assert result["input_token_positions"]==6 and result["unpadded_input_tokens"]==4
    assert result["provider_billable_tokens"]==0 and result["status"]=="RECONCILED"


def test_local_scoring_interruption_preserves_positions_and_unknown_latency(tmp_path):
    from benchmarks.lifecycle.ledger import recover_offline
    from benchmarks.lifecycle.report import local_scoring_accounting
    start={"attempt_id":"encoder","state":"STARTED","input_token_positions":8,"unpadded_input_tokens":6,"provider_billable_tokens":0}
    append(tmp_path/"local_scoring_attempts.jsonl",start)
    assert local_scoring_accounting(tmp_path)["status"]=="IN_FLIGHT_OR_INTERRUPTED"
    assert recover_offline(tmp_path) and not recover_offline(tmp_path)
    result=local_scoring_accounting(tmp_path)
    assert result["status"]=="RECONCILED" and result["interrupted_calls"]==1
    assert result["input_token_positions"]==8 and result["unknown_latency_calls"]==1
    assert result["latency_status"]=="KNOWN_LOWER_BOUND"
    append(tmp_path/"local_scoring_attempts.jsonl",{**start,"state":"FINISHED","status":"RETURNED","latency_ms":1})
    assert local_scoring_accounting(tmp_path)["status"]=="MISMATCH"


def test_monitored_compose_captures_output_and_cleans_up_on_failure(tmp_path,monkeypatch):
    import subprocess
    import sys
    from benchmarks.lifecycle import infrastructure
    runtime=tmp_path/".runtime"
    runtime.mkdir()
    (runtime/"compose.env").write_text("LOCOMO_PROJECT=engram-lifecycle-test\n")
    original=subprocess.Popen
    processes=[]
    def local_process(*args,**kwargs):
        process=original([sys.executable,"-c","import time; print('scored',flush=True); time.sleep(.6)"],**kwargs)
        processes.append(process)
        return process
    monkeypatch.setattr(infrastructure.subprocess,"Popen",local_process)
    monitor=Mock()
    result=infrastructure.compose(tmp_path,"run","scoring",capture=True,monitor=monitor)
    assert result.returncode==0 and result.stdout.strip()=="scored"
    monitor.assert_called_once()
    with pytest.raises(RuntimeError,match="service failed"):
        infrastructure.compose(tmp_path,"run","scoring",capture=True,monitor=Mock(side_effect=RuntimeError("service failed")))
    assert all(process.poll() is not None for process in processes)


def test_service_monitor_reports_missing_private_services(tmp_path,monkeypatch):
    from benchmarks.lifecycle import __main__ as host
    required=["postgres","neo4j","redis","temporal","worker","dispatcher","driver"]
    rows=[{"Service":s,"State":"paused" if s in {"worker","dispatcher"} else "running","Health":"healthy"} for s in required]
    compose_mock=Mock(return_value=SimpleNamespace(returncode=0,stdout="\n".join(json.dumps(r) for r in rows)))
    monkeypatch.setattr(host,"compose",compose_mock)
    host.monitor(tmp_path,"secondary_scoring")
    compose_mock.return_value.stdout="\n".join(json.dumps(r) for r in rows[:-1])
    with pytest.raises(RuntimeError,match="readiness failed"):
        host.monitor(tmp_path,"secondary_scoring")
    result=json.loads((tmp_path/"service_failure.json").read_text())
    assert result["missing"]==["driver"] and result["action"]=="secondary_scoring"


def test_external_scoring_phase_does_not_change_lifecycle_usage(tmp_path,monkeypatch):
    from benchmarks.lifecycle import runner
    progress=SimpleNamespace(update=Mock())
    runner.action(None,{"action":"phase","phase":"secondary_scoring"},tmp_path,progress)
    progress.update.assert_called_once_with(phase="secondary_scoring",current_question=None)
    progress.update.reset_mock()
    runner.action(None,{"action":"phase","phase":"complete","status":"COMPLETE"},tmp_path,progress)
    progress.update.assert_called_once_with(phase="complete",current_question=None,status="COMPLETE")


def test_ingestion_throughput_excludes_between_session_query_pauses():
    from benchmarks.lifecycle.report import ingestion_aggregates
    rows=[{"conversation_id":"c","session":s,"status":"COMPLETE","created_at":start,"processed_at":end,"raw_retention_verified":True,"pipeline_latency_seconds":10,"activity_stage_timings_ms":{"extract":2000},"extracted_triplets":2} for s,start,end in [(1,"2026-01-01T00:00:00+00:00","2026-01-01T00:00:10+00:00"),(2,"2026-01-01T00:10:00+00:00","2026-01-01T00:10:10+00:00")]]
    rows.append({"conversation_id":"c","session":3,"status":"ACCEPTED_NOT_VERIFIED","raw_retention_verified":None})
    result=ingestion_aggregates(rows)
    assert result["accepted_events"]==3 and result["throughput_event_denominator"]==2
    assert result["event_processing_span_seconds"]==20 and result["events_per_second"]==.1
    assert result["extracted_triplets_known_total"]==4 and result["extracted_triplets_unknown_events"]==1
    assert result["pipeline_latency_seconds"]["p99"]==10
    assert result["raw_retention_unknown_events"]==1


def test_progress_resumes_elapsed_work_and_phase_counts(tmp_path):
    from benchmarks.lifecycle.ledger import Progress
    atomic(tmp_path/"progress.json",{"elapsed_seconds":123,"known_tokens":456})
    progress=Progress(tmp_path,phase="ready")
    progress.update(phase="evaluation",completed=9)
    progress.update(phase="ingestion",completed=50)
    progress.close("STOPPED")
    result=json.loads((tmp_path/"progress.json").read_text())
    assert result["elapsed_seconds"]>=123 and result["known_tokens"]==456
    assert result["completed_evaluations"]==9 and result["accepted_ingestion_turns"]==50
    progress.update(status="COMPLETE",phase="complete")
    progress.close("STOPPED")
    assert json.loads((tmp_path/"progress.json").read_text())["status"]=="COMPLETE"


def test_crash_recovery_preserves_tail_and_accounts_unknown_attempt(tmp_path):
    from benchmarks.lifecycle.ledger import recover_offline
    start={"attempt_id":"interrupted","trace_id":"old","state":"STARTED","component":"query","operation_id":"op"}
    append(tmp_path/"provider_attempts.1.jsonl",start)
    path=tmp_path/"result_rows.jsonl"
    path.write_bytes(b'{"unfinished":')
    recovered=recover_offline(tmp_path)
    assert path.read_bytes()==b""
    assert next(tmp_path.glob("*.tail")).read_bytes()==b'{"unfinished":'
    assert recovered and not recover_offline(tmp_path)
    result=accounting(tmp_path)
    assert result["status"]=="RECONCILED"
    assert result["components"]["query"]["unknown_usage_calls"]==1
    assert result["recovered_interrupted_attempt_ids"]==["interrupted"]


def test_completed_question_includes_prior_attempt_consumption(tmp_path):
    from benchmarks.lifecycle.report import completed_rows
    append(tmp_path/"result_rows.jsonl",{"evaluation_id":"q1","trace_id":"new","qa_id":"q1","checkpoint_variant":"completed","question_status":"VALID","total_tokens":5,"known_total_tokens":5,"unknown_usage_calls":0})
    append(tmp_path/"evaluation_attempts.jsonl",{"evaluation_id":"q1","trace_id":"old","state":"STARTED"})
    append(tmp_path/"evaluation_attempts.jsonl",{"evaluation_id":"q1","trace_id":"new","state":"STARTED"})
    call={"attempt_id":"earlier","trace_id":"old","state":"FINISHED","component":"query","operation_id":"op","input_tokens":10,"output_tokens":2,"token_count_source":"provider"}
    append(tmp_path/"provider_attempts.1.jsonl",call)
    row=completed_rows(tmp_path)[0]
    assert row["known_total_tokens"]==17 and row["total_tokens"]==17
    assert row["prior_attempt_known_tokens"]==12 and row["resubmission_count"]==1


def test_generation_failure_keeps_independent_retrieval_measurements(data):
    from benchmarks.lifecycle.evaluation import make_row
    from benchmarks.lifecycle.adapter import Provenance
    conv=data[0]
    evaluation=next(e for e in dataset.evaluations(conv,dataset.select(data,"pilot")) if e.question.positive)
    p=Provenance({"postgres":{}},[{"conversation_id":conv.id,"event_id":"source","dia_id":evaluation.question.evidence[0]}],conv)
    trace=Trace("generation-failure")
    trace.emit("tool",level="L1",function="current_session_search",result={"text":"original","sources":[{"event_id":"source"}]})
    trace.emit("model_context",text="original")
    result=SimpleNamespace(answer="The answer generator is temporarily unavailable.",retrieval_metadata=SimpleNamespace(to_dict=lambda:{"answer_mode":"provider-unavailable"}))
    row=make_row(evaluation,trace,p,result,run_id="test",git_sha="test",config_hash="test")
    assert row["status"]=="GENERATION_ERROR" and row["official_locomo_f1"]==0
    assert row["retrieval_status"]=="COMPLETE" and row["any_evidence_recalled"]


def test_token_estimates_are_not_claimed_as_known_provider_usage():
    result=response('{"store":true}')
    result.usage=None
    p=provider([result])
    with capture(Trace("estimate")) as trace:
        p.complete(system_prompt="JSON",user_prompt="fact")
    usage=summarize_usage(trace.calls)
    assert usage["total_tokens"]>0 and usage["known_total_tokens"]==0
    assert usage["estimated_total_tokens"]==usage["total_tokens"] and usage["estimated_usage_calls"]==1
    assert usage["token_count_source"]=="provider_and_estimate"


def test_routing_compatibility_requires_a_real_store_read(data):
    from benchmarks.lifecycle.evaluation import make_row
    from benchmarks.lifecycle.adapter import Provenance
    conv=data[0]
    evaluation=next(e for e in dataset.evaluations(conv,dataset.select(data,"pilot")) if e.expected_level=="L2")
    p=Provenance({"postgres":{}},[],conv)
    trace=Trace("unsupported",stage="L2",executed=["L2"])
    trace.emit("tool",function="past_session_search",level="L2",result={"text":"","sources":[]})
    row=make_row(evaluation,trace,p,run_id="test",git_sha="test",config_hash="test")
    assert not row["routing_correct"] and row["read_source_levels"]==[]
    trace.emit("store",store="postgres_conversations",function="search_session_events",result=[])
    row=make_row(evaluation,trace,p,run_id="test",git_sha="test",config_hash="test")
    assert row["routing_correct"] and row["read_source_levels"]==["L2"]


def test_partially_unmapped_claim_lineage_is_not_exact(data):
    from benchmarks.lifecycle.adapter import Provenance
    conv=data[0]
    pg={"memory_claims":[{"id":"c","source_event_id":"known"}],"memory_evidence":[{"id":"e","claim_id":"c","source_event_id":"unknown"}]}
    p=Provenance({"postgres":pg},[{"conversation_id":conv.id,"event_id":"known","dia_id":"D1:1"}],conv)
    assert p.row({"claim_id":"c"})==(["D1:1"],False)


def test_returned_and_packed_tiers_do_not_depend_on_provenance(data):
    from benchmarks.lifecycle.adapter import Provenance
    from benchmarks.lifecycle.evaluation import context_evidence
    p=Provenance({"postgres":{}},[],data[0])
    trace=Trace("coarse")
    row={"memory_id":"coarse","claim_id":None,"body":"derived overview"}
    trace.emit("tool",function="facts_search",level="L3_FACTS",result=[[row],1])
    trace.emit("tool",function="graph_search",level="L3_GRAPH",result=[[],0])
    trace.emit("packing",inputs=[{},[row]],result=[row])
    trace.emit("model_context",text="derived overview")
    result=context_evidence(p,trace,{})
    assert result["retrieval_source_levels"]==["L3_FACTS"]
    assert result["supplied_context_levels"]==["L3_FACTS"]
    assert result["provenance_status"]=="INCOMPLETE"


def test_observation_preserves_signature_for_native_invocation():
    from benchmarks.lifecycle.adapter import Observed
    from engram.retrieval.orchestrator import _invoke
    class Vector:
        def vector_search(self,query_embedding,k=10,*,tenant_id=None):
            return {"embedding":query_embedding,"k":k,"tenant_id":tenant_id}
    proxy=Observed(Vector(),"neo4j")
    result=_invoke(proxy.vector_search,{"embedding":[9],"query_embedding":[1,2],"k":20,"tenant_id":"private","timeout_s":3})
    assert result=={"embedding":[1,2],"k":20,"tenant_id":"private"}


def test_duplicate_utterances_truncated_context_have_exact_turn_provenance():
    from benchmarks.lifecycle.adapter import Provenance,session_id
    from engram.session.manager import SessionState,Turn
    turns=(dataset.Turn("D1:1",1,0,"Alice","Repeated","","2023-01-01T00:00:00+00:00","date"),dataset.Turn("D1:2",1,1,"Alice","Repeated","","2023-01-01T00:00:00+00:00","date"))
    conv=dataset.Conversation("conv-test",turns,())
    sid=session_id(conv.id,1)
    state=SessionState(sid,turns=[Turn("user",t.content,t.timestamp,t.index*2) for t in turns])
    p=Provenance({"postgres":{"session_states":[{"session_id":sid,"payload":state.to_payload()}]}},[],conv)
    ids,exact,_=p.context({"text":"USER: "+turns[-1].content,"sources":[{"session_id":sid,"source_type":"postgresql_session"}]})
    assert ids==["D1:2"] and exact
