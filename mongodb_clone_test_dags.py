from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
import json
import os
import subprocess

default_args = {
    'owner': 'devops',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

# 함수 팩토리를 사용하여 각 서비스와 환경에 대한 고유한 함수 생성
def create_temp_dir_factory():
    """임시 디렉토리 생성 함수를 반환"""
    def create_temp_dir(**kwargs):
        temp_dir = f"/tmp/test_mongodb_{kwargs['ds_nodash']}"
        try:
            os.makedirs(temp_dir, exist_ok=True)
            print(f"[테스트] 임시 디렉토리 생성: {temp_dir}")
            return temp_dir
        except Exception as e:
            print(f"[테스트] 임시 디렉토리 생성 실패: {e}")
            raise
    return create_temp_dir

def check_git_repo_factory(git_repo, git_branch):
    """Git 저장소 확인 함수를 반환"""
    def check_git_repo(**kwargs):
        ti = kwargs['ti']
        temp_dir = ti.xcom_pull(task_ids='step1_create_temp_dir')
        
        print(f"[테스트] Git 저장소 정보:")
        print(f"[테스트] - 저장소: {git_repo}")
        print(f"[테스트] - 브랜치: {git_branch}")
        print(f"[테스트] - 임시 디렉토리: {temp_dir}")
        
        # 실제 클론하지 않고 성공으로 처리
        repo_dir = f"{temp_dir}/repo"
        print(f"[테스트] Git 클론 명령 (실행되지 않음): git clone --depth 1 --branch {git_branch} {git_repo}")
        print(f"[테스트] 가상 저장소 경로: {repo_dir}")
        
        return repo_dir
    return check_git_repo

def check_script_files_factory(service, src_env, tgt_env, script, config):
    """스크립트 파일 확인 함수를 반환"""
    def check_script_files(**kwargs):
        ti = kwargs['ti']
        repo_dir = ti.xcom_pull(task_ids='step2_check_git_repo')
        
        # 실제 파일 확인 대신 가상 경로 출력
        script_path = f"{repo_dir}/mongodb/clone/{script}"
        slack_path = f"{repo_dir}/mongodb/clone/slack_alert.py"
        
        print(f"""
[테스트] 스크립트 파일 정보 확인
==============================
서비스: {service}
소스 환경: {src_env}
타겟 환경: {tgt_env}
스크립트: {script}
스크립트 경로: {script_path}
슬랙 알림 경로: {slack_path}
==============================

[테스트] 환경 변수 정보:
- ATLAS_GROUP_ID: {config.get('services', {}).get(service, {}).get(src_env, {}).get('atlas_group_id', '')}
- ATLAS_CLUSTER_NAME: {config.get('services', {}).get(service, {}).get(src_env, {}).get('atlas_cluster_name', '')}
- ATLAS_TARGET_GROUP_ID: {config.get('services', {}).get(service, {}).get(tgt_env, {}).get('atlas_group_id', '')}
- ATLAS_TARGET_CLUSTER_NAME: {config.get('services', {}).get(service, {}).get(tgt_env, {}).get('atlas_cluster_name', '')}
- SLACK_APP_CHANNEL: {config.get('slack', {}).get('channel', '')}

[테스트] 만약 실제 실행된다면, 다음 명령이 실행됩니다:
cd {repo_dir}/mongodb/clone
python {script}

[테스트] 하지만 실제로는 실행되지 않습니다! (테스트 모드)
""")
        return "테스트 완료"
    return check_script_files

def cleanup_temp_dir_factory():
    """임시 디렉토리 정리 함수를 반환"""
    def cleanup_temp_dir(**kwargs):
        ti = kwargs['ti']
        temp_dir = ti.xcom_pull(task_ids='step1_create_temp_dir')
        
        print(f"[테스트] 임시 디렉토리 정리: {temp_dir}")
        # 실제 삭제 대신 메시지만 출력
        print(f"[테스트] 명령 (실행되지 않음): rm -rf {temp_dir}")
        
        return "정리 완료"
    return cleanup_temp_dir

try:
    mongodb_clone_config_str = Variable.get('mongodb_clone_config')
    mongodb_clone_config = json.loads(mongodb_clone_config_str)
    
    GIT_REPO = mongodb_clone_config.get('git', {}).get('repo', "https://github.com/your-organization/devops-batch.git")
    GIT_BRANCH = mongodb_clone_config.get('git', {}).get('branch', "main")
    
    TASKS = mongodb_clone_config.get('tasks', [])
    print(f"[테스트] MongoDB 클론 작업 설정 로드됨: 총 {len(TASKS)}개 작업")
    
except Exception as e:
    error_message = f"[테스트] 오류: 'mongodb_clone_config' 변수 설정 문제. 상세: {str(e)}"
    print(error_message)
    TASKS = []

if not TASKS:
    print("[테스트] 작업 목록이 비어있어 DAG를 생성하지 않습니다.")

# 각 작업 조합별로 테스트 DAG 생성
for idx, task_config in enumerate(TASKS):
    service, src_env, tgt_env, script, schedule = task_config
    
    dag_id = f'test_mongodb_{service}_{src_env}_{tgt_env}_clone'
    
    if script == 'clone_task.py':
        task_type = "스냅샷 생성 및 복제"
    else:
        task_type = "스냅샷 복원"
    
    dag_description = f"[테스트] MongoDB {service.upper()} {src_env.upper()} → {tgt_env.upper()} {task_type}"
    
    # 각 서비스/환경에 맞는 함수 생성
    create_temp_dir = create_temp_dir_factory()
    check_git_repo = check_git_repo_factory(GIT_REPO, GIT_BRANCH)
    check_script_files = check_script_files_factory(service, src_env, tgt_env, script, mongodb_clone_config)
    cleanup_temp_dir = cleanup_temp_dir_factory()
    
    with DAG(
        dag_id,
        default_args=default_args,
        description=dag_description,
        schedule_interval=schedule,  # 테스트도 실제 스케줄로 실행
        catchup=False,
        tags=['test', 'mongodb', 'clone', service, src_env, tgt_env],
    ) as dag:
        
        # 임시 디렉토리 생성
        step1 = PythonOperator(
            task_id='step1_create_temp_dir',
            python_callable=create_temp_dir,
            provide_context=True,
        )
        
        # Git 저장소 확인 (클론 없음)
        step2 = PythonOperator(
            task_id='step2_check_git_repo',
            python_callable=check_git_repo,
            provide_context=True,
        )
        
        # 스크립트 파일 확인
        step3 = PythonOperator(
            task_id=f'step3_check_{service}_{src_env}_{tgt_env}_script',
            python_callable=check_script_files,
            provide_context=True,
        )
        
        # 임시 파일 정리
        step4 = PythonOperator(
            task_id='step4_cleanup',
            python_callable=cleanup_temp_dir,
            provide_context=True,
            trigger_rule=TriggerRule.ALL_DONE,
        )
        
        # 작업 순서 정의
        step1 >> step2 >> step3 >> step4
        
        # DAG 변수에 현재 DAG 할당
        globals()[dag_id] = dag