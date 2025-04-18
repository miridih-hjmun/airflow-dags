from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
import json

default_args = {
    'owner': 'devops',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['hjmun@miridih.com'],
}

try:
    # Airflow 변수에서 RDS 클론 설정 가져오기
    rds_clone_config_str = Variable.get('rds_clone_config')
    rds_clone_config = json.loads(rds_clone_config_str)
    
    GIT_REPO = rds_clone_config.get('git', {}).get('repo', "https://github.com/miridih/devops-docker.git")
    GIT_BRANCH = rds_clone_config.get('git', {}).get('branch', "main")
    
    TASKS = rds_clone_config.get('tasks', [])
    print(f"RDS 클론 작업 설정을 로드했습니다. 총 {len(TASKS)}개 작업이 정의되어 있습니다.")
    
except Exception as e:
    error_message = f"오류: 'rds_clone_config' 변수가 설정되지 않았거나 형식이 잘못되었습니다. 오류 상세 정보: {str(e)}"
    print(error_message)
    TASKS = []

if not TASKS:
    print("작업 목록이 비어있어 DAG를 생성하지 않습니다.")

# 각 작업 조합별로 독립적인 DAG 생성
for idx, task_config in enumerate(TASKS):
    # 명확한 변수명으로 구성 요소 추출
    services, env, script_path, schedule = task_config
    
    # DAG ID 생성 (고유 식별자) - 테스트 버전임을 명시
    dag_id = f'test_rds_{services}_{env}_clone'
    
    # 읽기 쉬운 DAG 설명 생성
    dag_description = f"[테스트 전용] RDS {services.upper()} PROD → {env.upper()} 클론 작업"
    
    with DAG(
        dag_id,
        default_args=default_args,
        description=dag_description,
        schedule_interval=None,  # 테스트용이므로 수동 트리거만 가능하게 설정
        catchup=False,
        tags=['test', 'devops', 'rds', 'clone', services, env],
    ) as dag:
        
        # 모든 작업을 하나의 작업으로 통합
        run_all = BashOperator(
            task_id=f'run_{services}_{env}_clone_all',
            bash_command=f'''
            # 1. 임시 디렉토리 생성
            TEMP_DIR="${{AIRFLOW_HOME}}/tmp/$(date +%Y%m%d_%H%M%S)_rdsclone_test"
            echo "임시 디렉토리 생성: $TEMP_DIR"
            mkdir -p "$TEMP_DIR"
            
            # 2. Git 저장소에서 코드 가져오기
            echo "Git 저장소 클론 중: {GIT_REPO} (브랜치: {GIT_BRANCH})"
            cd $TEMP_DIR
            git clone --depth 1 --branch {GIT_BRANCH} {GIT_REPO} repo
            
            # 클론 결과 확인
            if [ $? -ne 0 ]; then
                echo "❌ Git 저장소 클론 실패"
                rm -rf "$TEMP_DIR"
                exit 1
            fi
            
            echo "저장소 내용:"
            ls -la "$TEMP_DIR/repo"
            
            # 3. RDS 클론 스크립트 실행 가능성 테스트
            echo "RDS 클론 스크립트 확인 중..."
            cd "$TEMP_DIR/repo/{script_path}"
            
            echo "============== RDS 클론 실행 가능성 테스트 =============="
            echo "작업 경로: $TEMP_DIR/repo/{script_path}"
            
            # RdsToRds.sh 파일 확인
            if [ -f "RdsToRds.sh" ]; then
                echo "✅ RdsToRds.sh 파일 존재: OK"
                # 내용 확인 (처음 몇 줄만)
                echo "RdsToRds.sh 내용 (첫 10줄):"
                head -n 10 RdsToRds.sh
                
                # 실행 권한 확인
                if [ -x "RdsToRds.sh" ]; then
                    echo "✅ RdsToRds.sh 실행 권한: OK"
                else
                    echo "❌ RdsToRds.sh 실행 권한 없음 (chmod +x RdsToRds.sh로 권한 부여 필요)"
                fi
                
                # RdsToRds.sh 내 명령 확인
                echo ""
                echo "============== RdsToRds.sh 내부 파이썬 실행 명령 =============="
                grep -E "python|/usr/bin/python|/usr/local/bin/python" RdsToRds.sh || echo "Python 실행 명령을 찾을 수 없습니다."
                
                # 환경 변수 확인
                echo ""
                echo "============== RdsToRds.sh 내 환경 변수 확인 =============="
                grep -E "^[A-Z_]+=.*" RdsToRds.sh || echo "환경 변수를 찾을 수 없습니다."
                
                echo ""
                echo "============== 실행 시뮬레이션 =============="
                echo "다음 명령이 실행될 예정입니다 (실제로 실행되지 않음):"
                echo "./RdsToRds.sh"
                echo ""
                echo "✅ RdsToRds.sh 실행 가능성 테스트 완료"
            else
                echo "❌ RdsToRds.sh 파일 없음: ERROR"
                echo "디렉토리 내용:"
                ls -la
                echo "이 경로에 RdsToRds.sh 파일이 없습니다. 경로를 확인해주세요."
                echo ""
                echo "❌ RdsToRds.sh 실행 가능성 테스트 실패"
            fi
            
            # 4. 임시 파일 정리
            echo "임시 디렉토리 정리 중: $TEMP_DIR"
            cd /
            rm -rf "$TEMP_DIR"
            
            echo "모든 작업 완료"
            ''',
            execution_timeout=timedelta(hours=1),  # 테스트는 짧게 설정
        )
        
        # DAG 변수에 현재 DAG 할당
        globals()[dag_id] = dag 