"""Parse Spark event logs and print execution/job/stage/task summaries."""

import json
import logging
import time
from collections import defaultdict, Counter, OrderedDict

logger = logging.getLogger(__name__)

VERBOSE = False

#  *args - any number of positional argumenst
def vlog(msg, *args):
    '''Prints Verbose level info'''
    if VERBOSE:
        logger.info(msg, *args)


def to_int(x):
    '''
    Convert value to integer
    
    Args:
        x: numeric value
    
    Return:
        Value as integer or None if missing/invalid
    '''
    if x is None or x == {}:
        return None
    
    try:
        return(int(x))
    except (TypeError, ValueError):
        vlog("to_int conversion failed for value=%r", x)
        return None



def calc_duration(value: int, input_format:str="ms"):
    '''
    Convert duration to seconds. 
    
    Args:
        value: Duration value from event log (in ms or ns)
        input_format: "ms" or "ns"

    Returns:
        Seconds as float or None if missing/invalid.
    '''

    if value is None:
        return None
    
    v = to_int(value)
    if v is None:
        return None
    
 
    if input_format=="ms":
        return round(v/1000, 1)
            
    elif input_format=="ns":
        return round(v/1_000_000_000.0, 2) # I use numeric literal with underscores for readability
    
    logger.info("Unknown input_format=%s", input_format)
    return None


def dur_to_text(value:int):
    '''
    Hide if < 1 sec, add "s" for notation 

    Args:
        value: duration value from event log
    Returns:
        Seconds as text
    '''
    if value is None:
        return "n/a"

    if value<1:
        ret = "<1 s"
    else:
        ret = f"{str(value)} s"
    return ret


def fmt_seconds(value, input_format:str="ms"):
    '''
    Format duration to human-readable string
    '''
    sec = calc_duration(value,input_format=input_format)
    return dur_to_text(sec) if sec is not None else "n/a"


def fmt_int(x):
    '''
    Format value to int
    '''
    v = to_int(x)
    return v if v is not None else 0


def fmt_kb(byte_value):
    '''
    Convert bytes to kilobytes
    '''
    b = to_int(byte_value)

    if b is None:
        return "n/a"
    
    return f"{round(b / 1024, 1)}"

DURATION_METRICS_MS = {"scan time","duration","sort time","fetch wait time","task commit time"}
DURATION_METRICS_NS = {"shuffle write time"}

def format_metric_value(metric_name, raw_value):
    '''
    format metric value, miliseconds or nanoseconds
    '''
    name = (metric_name or "").strip()
    
    if name in DURATION_METRICS_MS:
        return dur_to_text(calc_duration(raw_value, "ms"))
    
    if name in DURATION_METRICS_NS:
        return dur_to_text(calc_duration(raw_value,"ns"))
    
    return raw_value


def path_key(path):
    '''
    Convert string path_key to sortable integers
    '''
    # "0.1.10.2" -> (0,1,10,2)
    if path is None:
        return (0,)
    parts = str(path).split(".")
    out = []

    for p in parts:
        try:
            out.append(int(p))
        except ValueError:
            out.append(0)
    return tuple(out)

def sort_dict(dict_name: dict, order_by_item: str="node_path")-> dict:
    '''
    Sort dictionary by path key
    '''
    return OrderedDict(sorted(dict_name.items() ,key =lambda kv: path_key(kv[1].get(order_by_item,"0"))))


def make_stores():
    '''
    Create storage structures
    '''

    vlog("Stores created")
    return {
        "executions_by_id":{} ,
        "jobs_by_id": {} ,
        "jobs_by_execution": defaultdict(list) ,
        "stages_by_id": {} ,
        "stages_to_job": defaultdict(list) ,
        "stages_by_job": defaultdict(list) ,
        "tasks_by_id": {} ,
        "tasks_by_stage": defaultdict(list) ,
        "metric_index": {} ,
        "plan_nodes": {} ,
        "accum_updates_by_execution":{} , # that cones from accumUpdates Event
        "stage_to_plan_metrics": defaultdict(list) ,

    }


def walk(node, S , node_path = "0"):
    '''
    Extract plan nodes and related metrics
    '''


    node_name = node.get("nodeName")
    simple_string = node.get("simpleString")
    meta = node.get("metadata") or {}
    children = node.get("children") or []
    file_location = meta.get("Location") or ""

    parts = str(file_location).rsplit("/", maxsplit=2)

    if len(parts )>=2:
        file_name = parts[-1].replace("]","")
        schema_name = parts[-2]
    else:
        file_name = ""
        schema_name = ""

    
    
    S["plan_nodes"][node_path] = {
        "node_name":node_name ,
        "simple_string": simple_string ,
        "metadata": meta ,
        "file_name": file_name ,
        "schema_name": schema_name 
        

    }

    for m in node.get("metrics") or []:
        acc_id =to_int(m.get("accumulatorId"))
        if acc_id is None:
            continue
        S["metric_index"][acc_id]={
            "node_name": node_name ,
            "node_path": node_path ,
            "metric_name": m.get("name") ,
            "metric_type": m.get("metricType") ,
            "file_name":file_name ,
            "schema_name": schema_name
        }
        

    for i,ch in enumerate(children or []):
        walk(ch,S , f"{node_path}.{i}")



def handle_exec_start(e, S):
    '''
    Extract execution start info from event log
    '''
    
    execution_id = e.get("executionId")
    root_execution_id =  e.get("rootExecutionId")
    description = e.get("description")
    stat_id = (description.split("statement ",1)[1].split(":")[0] if "statement " in description else None)
    is_write = (True if "write.mode" in description else False)
    
    epoch_ms = e.get("time")
    epoch_s = epoch_ms/1000.0
    formatted_time = time.strftime('%d-%m-%Y %H:%M:%S',time.gmtime(epoch_s))

    rec = S["executions_by_id"].get(execution_id,{"execution_id":execution_id})
    rec.update({
        "execution_id": execution_id,
        "root_execution_id": root_execution_id,
        "statement_id": stat_id,
        "description": description,
        "epoch_start_time": epoch_s,
        "start_time":formatted_time ,
        "has_write_op": is_write ,
    })

    S["executions_by_id"][execution_id]=rec
    vlog("Handling start of sql execution Id %s",execution_id)


def handle_exec_end(e, S):
    '''
    Extract execution end info from event log
    '''
    execution_id = e.get("executionId")
    epoch_ms = e.get("time")
    epoch_s = epoch_ms/1000.0
    formatted_end_time = time.strftime('%d-%m-%Y %H:%M:%S',time.gmtime(epoch_s))

    rec = S["executions_by_id"].get(execution_id,{"execution_id": execution_id})
    rec.update({
        "epoch_end_time":epoch_s,
        "end_time":formatted_end_time
    })

    S["executions_by_id"][execution_id]=rec
    vlog("Handling end of sql execution Id %s", execution_id)


def handle_job_start(e, S):
    '''
    Extract job start info from event log
    '''
    props = e.get("Properties") or {}
    job_id = to_int(e.get("Job ID"))
    exec_id = to_int(props.get("spark.sql.execution.id"))
    job_root_sql_exec_id = to_int(props.get("spark.sql.execution.root.id"))
    job_desc = props.get("spark.job.description") or ""
    stage_ids = e.get("Stage IDs") or []
    submission_ms = e.get("Submission Time")


    rec = S["jobs_by_id"].get(job_id,{"job_id":job_id})

    rec.update({
            "execution_id": exec_id ,
            "root_execution_id":job_root_sql_exec_id ,
            "stage_ids": stage_ids,
            "submission_ms":submission_ms ,
            "description":job_desc[0:200]
    })
        

    S["jobs_by_id"][job_id]=rec

    for sid in stage_ids:
        sid_int = to_int(sid)
        if sid_int is not None:
            S["stages_to_job"][sid_int].append(job_id)

    if exec_id is not None:
        S["jobs_by_execution"][exec_id].append(job_id)
    
    vlog("  Handling start of job %s for sql execution id %s", job_id,exec_id)
    


def handle_job_end(e, S):
    '''
    Extract job end info from event log
    '''
    job_id = to_int(e.get("Job ID"))
    completion_ms = e.get("Completion Time")
    job_result = e.get("Job Result")
    result = job_result.get("Result")
            
    rec = S["jobs_by_id"].get(job_id,{"job_id":job_id})
    rec.update({
                "completion_ms":completion_ms,
                "result":result
            })

    if completion_ms is not None and rec.get("submission_ms") is not None:
        rec["duration_ms"] = completion_ms-rec.get("submission_ms")



    S["jobs_by_id"][job_id]=rec

    vlog("  Handling end of job %s",job_id)



def handle_stage_submitted(e, S):
    '''
    Extract stage submission info from event log
    '''

    stage_info = e.get("Stage Info") or {}
    stage_id = to_int(stage_info.get("Stage ID"))
    stage_attempt_id = to_int(stage_info.get("Stage Attempt ID"))

     # in case one stage belongs to multiple jobs       
    candidates = S["stages_to_job"].get(stage_id, [])
    # take last job
    job_id = candidates[-1] if candidates else None
    if job_id is None:
        return f"{stage_id} is not mapped to any job"
     
    rdd_ids, rdd_types,scope_names = [], [], []
            
    for rdd in (stage_info.get("RDD Info") or []):
        
        rdd_ids.append(rdd.get("RDD ID"))
        rdd_types.append(rdd.get("Name"))
                
        scope = rdd.get("Scope","")
        scope_name = scope.split(":")[-1].replace('"',"").replace("}","").strip()
        if scope_name:
            scope_names.append(scope_name )
                            
                           
    rdd_types = dict(Counter(rdd_types))
    scope_names_uq= sorted(set(scope_names))

    rec = S["stages_by_id"].get((stage_id, stage_attempt_id), {})
    rec.update({
                            "stage_id":stage_id ,
                            "stage_attempt_id":stage_attempt_id ,
                            "stage_name": stage_info.get("Stage Name") ,
                            "num_tasks_planned": stage_info.get("Number of Tasks") ,
                            "submission_ms": stage_info.get("Submission Time") ,
                            "scope_names":  scope_names_uq ,
                            "rdd_ids":rdd_ids ,
                            "rdd_types": rdd_types ,
                            "num_files_scanned": dict(rdd_types).get("FileScanRDD",0) ,
                            "has_scan_parquet": any("Scan parquet" in s for s in scope_names_uq) ,
                            "has_broadcasted_data": any("BroadcastExchange" in s for s in scope_names_uq) ,
                            "has_whole_stage_codegen":any("WholeStageCodegen" in s for s in scope_names_uq) ,
                            "job_id": job_id


                        })
        
    S["stages_by_id"][(stage_id, stage_attempt_id)] = rec

    S["stages_by_job"][job_id].append((stage_id, stage_attempt_id))
    
    vlog("  Handling start of stage Id %s.%s for job id %s",stage_id,stage_attempt_id,job_id)


def handle_stage_completed(e, S):
    '''
    Extract stage completion info from event log
    '''

    stage_info = e.get("Stage Info")
    stage_id = to_int(stage_info.get("Stage ID"))
    stage_attempt_id = to_int(stage_info.get("Stage Attempt ID"))
            

    rec = S["stages_by_id"].get((stage_id, stage_attempt_id),{
                "stage_id":stage_id,
                "stage_attempt_id":stage_attempt_id

            })
            
    submission_ms = stage_info.get("Submission Time")
    completion_ms = stage_info.get("Completion Time")
    rec["completion_ms"]=completion_ms

    if submission_ms is not None and completion_ms is not None:
        rec["duration_ms"] = completion_ms-submission_ms

    if rec.get("submission_ms") is None:
        rec["submission_ms"] = submission_ms

    metrics = {}

    for a in (stage_info.get("Accumulables") or []):
        accumulator_id  = to_int(a.get("ID"))
             
        if accumulator_id is None:
            continue

        metrics[accumulator_id] = a.get("Value")
               
            
    rec["stage_metrics_raw"] = metrics

    S["stages_by_id"][(stage_id, stage_attempt_id)] = rec

    vlog("  Handling completion of stage id %s.%s", stage_id,stage_attempt_id)



def handle_final_plan(e, S):
    '''
    Extract nodes and metrics from final plan
    '''
    spark_plan_info = e.get("sparkPlanInfo") or {}
    if spark_plan_info.get("simpleString") =="AdaptiveSparkPlan isFinalPlan=true":
        #print("CALLING PLAN WALK, isFinalPlan =", e["sparkPlanInfo"]["simpleString"])
        walk(spark_plan_info, S)
        #print("PLAN WALK DONE, plan_nodes size =", len(S["plan_nodes"]))



def handle_task_start(e, S):
    '''
    Extract task start info from event log
    '''

    stage_id = to_int(e.get("Stage ID"))
    stage_attempt_id = to_int(e.get("Stage Attempt ID"))
    task_info = e.get("Task Info")

    task_id = to_int(task_info.get("Task ID"))
    task_attempt_id = to_int(task_info.get("Attempt"))

    if stage_id is None or stage_attempt_id is None or task_id is None or task_attempt_id is None:
        return
    
    key = (stage_id, stage_attempt_id, task_id, task_attempt_id)

    rec = S["tasks_by_id"].get(key,{
                "stage_id":stage_id ,
                "stage_attempt_id": stage_attempt_id ,
                "task_id": task_id ,
                "task_attempt_id": task_attempt_id
            } )

    rec.update({
                "partition_id": to_int(task_info.get("Partition ID")) ,
                "executor_id": task_info.get("Executor ID") ,
                "host": task_info.get("Host") ,
                "locality": task_info.get("Locality") ,
                "speculative": bool(task_info.get("Speculative")) ,
                "launch_ms": task_info.get("Launch Time")
             })

    S["tasks_by_id"][key] = rec

    vlog("  Handling start of task %s.%s for stage id %s.%s", task_id,task_attempt_id,stage_id,stage_attempt_id)



def handle_task_end(e, S):
    '''
    Extract task end info from event log
    '''

    stage_id = to_int(e.get("Stage ID"))
    stage_attempt_id = to_int(e.get("Stage Attempt ID"))
    task_info = e.get("Task Info")
    task_id = to_int(task_info.get("Task ID"))
    task_attempt_id = to_int(task_info.get("Attempt"))

    

    if stage_id is None or stage_attempt_id is None or task_id is None or task_attempt_id is None:
        return
                
    key = (stage_id, stage_attempt_id, task_id, task_attempt_id)        
    
    rec = S["tasks_by_id"].get((stage_id, stage_attempt_id, task_id, task_attempt_id),{
                "stage_id":stage_id ,
                "stage_attempt_id": stage_attempt_id ,
                "task_id": task_id ,
                "task_attempt_id": task_attempt_id
            } )

    finish_ms = task_info.get("Finish Time")
    launch_ms = rec.get("launch_ms") or to_int(task_info.get("Launch Time"))
    if finish_ms is not None and launch_ms is not None:
        rec["duration_ms"]= finish_ms-launch_ms
    rec["finish_ms"] = finish_ms

    rec["task_type"]= e.get("Task Type")

    rec["task_end_reason"] = e.get("Task End Reason",{}).get("Reason")

    task_metrics = e.get("Task Metrics") or {}
    
    rec.update({
                "exec_run_ms": to_int(task_metrics.get("Executor Run Time",0)) ,
                "exec_cpu_ns": to_int(task_metrics.get("Executor CPU Time",0)) ,
                "deserialize_ms": to_int(task_metrics.get("Executor Deserialize Time",0)) ,
                "gc_ms": to_int(task_metrics.get("JVM GC Time",0)) ,
                "mem_spill_bytes": to_int(task_metrics.get("Memory Bytes Spilled",0)) ,
                "disk_spill_bytes": to_int(task_metrics.get("Disk Bytes Spilled",0))
            

            })
    
    input_metrics = task_metrics.get("Input Metrics") or {}
    
    rec["input_bytes"]= fmt_int(input_metrics.get("Bytes Read",0))
    rec["input_records"] = fmt_int(input_metrics.get("Records Read",0))

    shuffle_read_metrics = task_metrics.get("Shuffle Read Metrics") or {}
    shuffle_write_metrics = task_metrics.get("Shuffle Write Metrics") or {}

    rec["shuffle_read_bytes"] = (
        fmt_int(shuffle_read_metrics.get("Remote Bytes Read",0)) + 
        fmt_int(shuffle_read_metrics.get("Local Bytes Read",0))
    )
    rec["shuffle_read_records"] = fmt_int(shuffle_read_metrics.get("Total Records Read",0))
    rec["shuffle_fetch_wait_ms"] = fmt_int(shuffle_read_metrics.get("Fetch Wait Time",0))
    rec["shuffle_write_bytes"] = fmt_int(shuffle_write_metrics.get("Shuffle Bytes Written",0))
    rec["shuffle_write_records"] = fmt_int(shuffle_write_metrics.get("Shuffle Records Written",0))
    rec["shuffle_write_time_ns"] = fmt_int(shuffle_write_metrics.get("Shuffle Write Time",0))

    sql_acc ={}

    for a in (task_info.get("Accumulables") or []):
        if a.get("Metadata") =="sql":
            acc_id = to_int(a.get("ID"))
            if acc_id is not None:
                sql_acc[acc_id] = a.get("Value")
            
    if sql_acc:
        rec["task_sql_acc_raw"] = sql_acc
    
    S["tasks_by_id"][key] = rec
    S["tasks_by_stage"][(stage_id,stage_attempt_id)].append(rec)

    vlog("  Handling end of task id %s.%s for stage id %s.%s", task_id,task_attempt_id,stage_id, stage_attempt_id)


def handle_accum_updates(e, S):
    '''
    Extract accumulator updated metrics from event log
    '''
    execution_id = to_int(e.get("executionId"))
    if execution_id is None:
        vlog("accum updates without execution id: %r", e.get("executionId"))
    
    updates = e.get("accumUpdates") or []
    acc_map = dict(updates)

    rec = S["accum_updates_by_execution"].get(execution_id, {"execution_id":execution_id})
    rec.update(acc_map)
    S["accum_updates_by_execution"][execution_id]=rec

    vlog("      Handling accumulators updates for execution id %s, cumulative count %s",
         execution_id, 
         len(S["accum_updates_by_execution"][execution_id])
         )



def print_top_slowest_stages(job_ids, S, top_n):
    '''Stage tells which operation is slow'''
    all_stages_recs = []
    for job_id in job_ids:
        for stage_key in S["stages_by_job"].get(job_id, []):
            st = S["stages_by_id"].get(stage_key)
            if st:
                all_stages_recs.append(st)
    
    slow_stages = sorted(

        all_stages_recs ,
        key = lambda r: to_int(r.get("duration_ms")) or -1 ,
        reverse = True

    )[:top_n]
    
    print("-- Top slowest stages -- ")
    for st in slow_stages:
        key = (st["stage_id"], st["stage_attempt_id"])
        task_count = len(S["tasks_by_stage"].get(key, []))
        print(
            f"  Stage {st['stage_id']}.{st['stage_attempt_id']} | "
            f"dur = {fmt_seconds(st.get('duration_ms'))} | tasks = {task_count} | "
            f"scan = {st.get('has_scan_parquet')} | broadcasted data = {st.get('has_broadcasted_data')}"
        )
    
    print()


def print_top_slowest_tasks(job_ids, S, top_n=10):
    '''
    Task shows why stage is slow
    '''
    stage_keys = set()
    for job_id in job_ids:
        stage_keys.update(S["stages_by_job"].get(job_id, []))
    
    tasks = []
    for sk in stage_keys:
        tasks.extend(get_stage_tasks(sk, S))

    slow_tasks = sorted(
        tasks ,
        key = lambda t: to_int(t.get("duration_ms")) or -1 ,
        reverse = True

    )[:top_n]

    print("-- Top slowest tasks --")
    for t in slow_tasks:

        print(
            f"  Task {t['task_id']}.{t['task_attempt_id']} | "
            f"stage = {t['stage_id']}.{t['stage_attempt_id']} | " 
            f"dur = {fmt_seconds(t.get('duration_ms'))} | "
            f"part = {t.get('partition_id')} | "
            f"loc = {t.get('locality')} | "
            f"inputKB = {fmt_kb(t.get('input_bytes'))} | "
            f"spill = {fmt_int(t.get('disk_spill_bytes'))}"

        )
    
    print()


def print_skew_suspects_for_execution(job_ids, S, ratio = 3.0, min_tasks = 4):
    '''
    Print detected skew candidates
    '''
    print(f"-- Skew suspects task(duration > {ratio} X median) -- ")

    any_found = False

    for job_id in job_ids:

        for stage_key in S["stages_by_job"].get(job_id, []):
            tasks = S["tasks_by_stage"].get(stage_key, [])
            if len(tasks) < min_tasks:
                continue

            durations = [to_int(t.get("duration_ms")) for t in tasks if to_int(t.get("duration_ms")) is not None]
            if len(durations) < min_tasks:
                continue

            durs_sorted = sorted(durations)
            median = durs_sorted[len(durs_sorted)//2]
            if median < 0:
                continue

            max_dur = max(durs_sorted)         
            if max_dur > ratio*median:
                any_found = True
                slowest = max(tasks, key=lambda t: to_int(t.get("duration_ms")) or -1)
                print(
                    f"  Stage {stage_key[0]}.{stage_key[1]} | "
                    f"tasks = {len(tasks)} | median = {fmt_seconds(median)} | max = {fmt_seconds(max_dur)} | "
                    f"slowtask = {slowest['task_id']}.{slowest['task_attempt_id']} | part = {slowest.get('partition_id')} "
                )
    
    if not any_found:
        print(" (none found)")

    print()


def print_slowest_files(job_ids, S, top_n = 5):
    '''
    Print slowest processed files
    '''
    file_durations = {}

    for job_id in job_ids:
        for stage_key in S["stages_by_job"].get(job_id, []):
            st = S["stages_by_id"].get(stage_key)
            if not st:
                continue

            dur = to_int(st.get("duration_ms")) or 0

            file_name = None

            for m in (S["stage_to_plan_metrics"] or {}).get(stage_key, []):
                if (m.get("node_name") or "").strip() =="Scan parquet" and m.get("file_name"):
                    file_name = m["file_name"]
                    break
            
            if not file_name:
                continue

            file_durations[file_name] = file_durations.get(file_name,0)+dur
    
    top = sorted(file_durations.items(), key = lambda kv: kv[1], reverse = True)[:top_n]
    
    print(
        "-- Slowest files (sum of stage durations where file was scanned) --"
    )
    if not top:
        print("(no file info captured)")
    else:
        for fname,dur in top:
            print(f"    {fname}: {fmt_seconds(dur)}")

    print()

def print_write_execution_ids(executions):
    '''
    Return list of execution ids that have write operation
    '''
    exec_ids = []
    for exec_id, value in executions.items():
        if value["has_write_op"]:
            exec_ids.append(exec_id)
        else:
            continue
    return exec_ids



def parse_eventlog(path):
    '''
    Parse application log events
    '''

    logging.info("Start %s",parse_eventlog.__name__)
    S = make_stores()

    handlers = {
    "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart": handle_exec_start ,
    "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd": handle_exec_end ,
    "SparkListenerJobStart": handle_job_start ,
    "SparkListenerJobEnd": handle_job_end ,
    "SparkListenerStageSubmitted": handle_stage_submitted ,
    "SparkListenerStageCompleted": handle_stage_completed ,
    "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate": handle_final_plan ,
    "SparkListenerTaskStart": handle_task_start ,
    "SparkListenerTaskEnd": handle_task_end ,
    "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates": handle_accum_updates ,
    }

    

    with open(path, encoding = "utf-8") as f:
        for line in f:
            line = line.strip()

            if not line:
                continue

            event = json.loads(line)
            event_log_name = event.get("Event") or ""
            handler = handlers.get(event_log_name)
            
            if handler:
                event_name = event_log_name.split('.')[-1]
                vlog("Handling event=%s with handler=%s",event_name,handler.__name__)
            
                handler(event, S)
                #logging.info("Handling event=%s with handler=%s",event_name,handler.__name__)
            
            
    #print("PARSE END", id(S), len(S["plan_nodes"]))
    build_stage_to_plan_metrics(S)

    logging.info("End %s", parse_eventlog.__name__)
    return S

def get_execution_job_ids(execution_id, S):
    '''
    Get the list of job_id's hat belong to execution id
    '''
    job_ids = list(S["jobs_by_execution"].get(execution_id,[]))
    job_ids.sort(key = lambda jid: S["jobs_by_id"].get(jid,{}).get("submission_ms") or 0)
    return job_ids

def print_execution_overview(execution_id, job_ids, S):
    '''
    Print execution header and quick summaries
    '''
    print()
    print("### Block 1")
    print(f"----------------------------------- Execution Id {execution_id} Overview (top slowest, skew) --------------------------------------------------------")

    vlog("Execution %s jobs = %s", execution_id, len(job_ids))

    print_top_slowest_stages(job_ids, S, top_n=5)
    print_top_slowest_tasks(job_ids,S, top_n=10)
    print_skew_suspects_for_execution(job_ids, S)


def print_job_header(job_id, S):
    '''
    Print one job line
    '''
    j = S["jobs_by_id"][job_id]
    # from job start event, generate set()
    expected_stages = {to_int(x) for x in (j.get("stage_ids") or []) if to_int(x) is not None}
    # from submitted stages
    submitted_stages = {sid for (sid, _) in S["stages_by_job"].get(job_id, [])}
    missing_stages = expected_stages - submitted_stages
    skipped = len(missing_stages)
    
    stage_keys = S["stages_by_job"].get(job_id, [])
    


    print()
    print("-- Job breakdown --")
    print(f"    Job id {job_id} | stages = {len(stage_keys)} | dur = {fmt_seconds(j.get('duration_ms'))}  | "
          f"result = {j.get('result')} | skipped stages = {skipped}"
    )
    
    return stage_keys


def get_job_stage_recs(job_id, S):
    '''
    Get stage records for a job
    '''
    stage_keys = S["stages_by_job"].get(job_id, [])
    stage_recs =[S["stages_by_id"][k] for k  in stage_keys if k in S["stages_by_id"]]
    stage_recs.sort(key = lambda r:to_int(r.get("stage_attempt_id",0)))
    return stage_recs

def print_stage_header(st_rec, tasks):
    '''
    Print single stage header
    '''

    
    print(f"        Stage {st_rec['stage_id']}.{st_rec['stage_attempt_id']} | "
          f"tasks = {len(tasks)} | dur = {fmt_seconds(st_rec.get('duration_ms'))} | "
          f"has scan = {st_rec.get('has_scan_parquet')} | has broadcasted data = {st_rec.get('has_broadcasted_data')}"
          )
    

def get_stage_tasks(stage_key, S):
    '''
    Get stage tasks
    '''
    return S["tasks_by_stage"].get(stage_key, [])

    
def print_tasks(tasks):
    '''
    Print tasks info
    '''
    
    tasks_sorted = sorted(
                tasks, 
                key=lambda t:(to_int(t.get("duration_ms") or -1)),
                reverse = True
                )
    
    print("-- Task breakdown --")

    for t in tasks_sorted:
        
        print(f"                Task {t['task_id']}.{t['task_attempt_id']} | part = {t.get('partition_id')} | "
              f"dur = {fmt_seconds(t.get('duration_ms'))} | loc = {t.get('locality')} | "
              f"inputRec = {fmt_int(t.get('input_records'))} inputKB = {fmt_kb(t.get('input_bytes'))} | "
              f"  shufR = {fmt_kb(t.get('shuffle_read_bytes'))} | spillDisk = {fmt_kb(t.get('disk_spill_bytes'))}")
    

def collect_stage_metrics(st_rec, metric_index_sorted):
    '''
    Match stage metric to node info.

    Return sorted dict of metrics with node info
    '''
    
    st_metric = st_rec.get("stage_metrics_raw") or {}
    out = []

    for acc_id, val in st_metric.items():
        mi = metric_index_sorted.get(to_int(acc_id))
        if not mi:
            continue

        out.append({
            "acc_id": acc_id ,
            "value": val ,
            "node_name": mi.get("node_name", "") ,
            "node_path": mi.get("node_path", "") ,
            "metric_name":mi.get("metric_name", "") ,
            "file_name": mi.get('file_name', "") ,
            "schema_name":mi.get("schema_name", "") ,
        })
    
    out.sort(key= lambda m: path_key(m.get("node_path","0")), reverse = True)
    return out
                    
def store_stage_metrics(stage_to_plan_metrics, stage_key, resolved_metrics):
    '''
    Store stage metrics
    '''
    stage_to_plan_metrics[stage_key].extend(resolved_metrics)


def build_stage_to_plan_metrics(S):
    '''
    Resolve stage accumulators to plan nodes and store in stage_to_plan_metrics
    '''
    metric_index_sorted = sort_dict(S["metric_index"], order_by_item="node_path")

    for (stage_id, stage_attempt_id), st in S["stages_by_id"].items():
        stage_key = (stage_id, stage_attempt_id)

        collected = collect_stage_metrics(st,metric_index_sorted)
        S["stage_to_plan_metrics"][stage_key].extend(collected)



def print_scan_parquet_metrics(metrics):
    '''
    Print scan parquet specific metrics
    '''
    
    scan_rows = None
    scan_time = None
    filter_rows = None
    filtered_out_after_scan = None
    filtered_out_pct = None
    file = None


    for m in metrics:
        node_name = (m.get("node_name") or "").strip()
        metric_name = (m.get("metric_name") or "").strip()
        
        if node_name == "Scan parquet":
            if metric_name == "number of output rows":
                scan_rows = to_int(m.get("value"))

            if metric_name == "scan time":
                scan_time = to_int(m.get("value"))
            
            file = m.get("file_name")
        
        if node_name == "Filter":
            if metric_name == "number of output rows":
                filter_rows = to_int(m.get("value"))
        

        

    #print once
    if scan_rows is None and scan_time is None:
        return
    if scan_rows is not None and filter_rows  is not None:
        filtered_out_after_scan  = scan_rows-filter_rows
        if filtered_out_after_scan >0:
            filtered_out_pct = 100-((filtered_out_after_scan/scan_rows)*100)
        else:
            filtered_out_pct = 0

       
    rows_txt = f"{scan_rows:,}".replace(",", " ") if scan_rows is not None else "n/a"
    time_txt = fmt_seconds(scan_time, input_format="ms") if scan_time is not None else "n/a"
    filter_rows_txt = f"{filter_rows:,}".replace(","," ") if filter_rows is not None else "n/a"
    filtered_out_after_scan_txt = f"{filtered_out_after_scan }".replace(","," ") if filtered_out_after_scan is not None else "n/a"
    filtered_out_pct_txt =  f"{round(filtered_out_pct,1)}" if filtered_out_pct is not None else "n/a"
    fname_txt = file or "n/a"
    
    print(f"            Scan parquet | rows out = {rows_txt} | time = {time_txt} | {fname_txt}")
    print(f"            Filter       | rows out = {filter_rows_txt} | filtered after scan = {filtered_out_after_scan_txt} | filtered after scan % = {filtered_out_pct_txt}")
    print()

    



def print_collected_stage_metrics(st_rec, collected_metrics):
    '''
    Print collected stage metrics
    '''
  
    #print(f"        Stage {st_rec['stage_id']}.{st_rec['stage_attempt_id']} metrics:")
    
    # printed_scan_file = False
    # seen_nodes = set()



    print_scan_parquet_metrics(collected_metrics)


     
#    for m in collected_metrics:
#        node_name = (m.get("node_name") or "").strip()
#        metric_name = (m.get("metric_name") or "").strip()
#        node_path = m.get("node_path","")
#        raw_val = m.get("value")
#
#        if  node_name == "Scan parquet":
#            if (not printed_scan_file):
#
#                 print(f"        file: {m.get('file_name','')}")
#
#        
#        if node_path not in seen_nodes:
#            print(f"            node {node_name} >> ")
#            seen_nodes.add(node_path)

#        metric_value = format_metric_value(metric_name, raw_val)
#        print(f"                {metric_name } is {metric_value}")



def trace_execution(execution_id: int, S, task_breakdown = False):
    '''
    Print readable breakdown of one sql execution:
    execution => jobs => stages => tasks => metrics

    Assumes parse_event_log() already built:
    - S["stages_by_job"]
    - S["tasks_by_stage"]
    - S["stage_to_plan_metrics] - resolved stage metrics witj plan node info
    '''
    job_ids = get_execution_job_ids(execution_id, S)

    print_execution_overview(execution_id, job_ids, S)
    print_slowest_files(job_ids,S,top_n=5)
    

   # print("TRACE START", id(S), len(S["plan_nodes"]))

    print()
    print("### Block 2")
    print(f"----------------------------------- Execution Id {execution_id} trace (jobs > stages > tasks > metrics) ---------------------------------------------")
    for job_id in job_ids:
        print_job_header(job_id, S)
        # print("-- Stage breakdown --")
        stage_recs = get_job_stage_recs(job_id, S)

        
        for st_rec in stage_recs:
            stage_key = (st_rec["stage_id"], st_rec["stage_attempt_id"])
            
            
            # tasks already colected
            tasks = get_stage_tasks(stage_key, S)
            print_stage_header(st_rec, tasks)
            if task_breakdown:
                print_tasks(tasks)

            # metrics aslready resolved during parsing
            collected_metrics = S["stage_to_plan_metrics"].get(stage_key, [])
            print_collected_stage_metrics(st_rec, collected_metrics)

    # end of execution summary, relies on stage_to_plan_metrics
    #print_slowest_files(job_ids,S,top_n=5)
                
                