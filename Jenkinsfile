pipeline {
    agent any

    triggers {
        cron('H */12 * * *')
        pollSCM('H */6 * * *')
    }

    options {
        skipDefaultCheckout()
    }

    environment {
        VENV = "${env.WORKSPACE}/py-utils-venv"
        SPECTRUM_CARER_VENV = "${env.WORKSPACE}/thl-spectrum-carer-venv"
        GRLIQ_CARER_VENV = "${env.WORKSPACE}/grliq-carer-venv"
        GR_CARER_VENV = "${env.WORKSPACE}/gr-carer-venv"

        INCITE_MOUNT_DIR = '/mnt/thl-incite'
        TMP_DIR = "${env.WORKSPACE}/tmp"
    }

    stages {
        stage('python versions') {
            matrix {
                axes {
                    axis {
                        name 'PYTHON_VERSION'
                        values 'python3.13', 'python3.12', 'python3.11', 'python3.10'
                    }
                }

                stages {
                    stage('Setup DB') {
                        steps {
                            script {
                                env.DB_NAME = 'unittest-thl-' + UUID.randomUUID().toString().replace('-', '').take(12)
                                env.THL_WEB_RW_DB = "postgres://${env.DB_USER}:${env.DB_PASSWORD}@${env.DB_POSTGRESQL_HOST}/${env.DB_NAME}"
                                env.THL_WEB_RR_DB = env.THL_WEB_RW_DB
                                env.THL_WEB_RO_DB = env.THL_WEB_RW_DB
                                echo "Using database: ${env.DB_NAME}"

                                env.SPECTRUM_DB_NAME = 'unittest-thl-spectrum-' + UUID.randomUUID().toString().replace('-', '').take(12)
                                env.SPECTRUM_RW_DB = "mariadb://${env.DB_USER}:${env.DB_PASSWORD}@${env.DB_MARIA_HOST}/${env.SPECTRUM_DB_NAME}"
                                env.SPECTRUM_RR_DB = env.SPECTRUM_RW_DB
                                echo "Using database: ${env.SPECTRUM_DB_NAME}"

                                env.GRLIQ_DB_NAME = 'unittest-grliq-' + UUID.randomUUID().toString().replace('-', '').take(12)
                                env.GRLIQ_DB = "postgres://${env.DB_USER}:${env.DB_PASSWORD}@${env.DB_POSTGRESQL_HOST}/${env.GRLIQ_DB_NAME}"
                                echo "Using database: ${env.GRLIQ_DB_NAME}"

                                env.GR_DB_NAME = 'unittest-gr-' + UUID.randomUUID().toString().replace('-', '').take(12)
                                env.GR_DB = "postgres://${env.DB_USER}:${env.DB_PASSWORD}@${env.DB_POSTGRESQL_HOST}/${env.GR_DB_NAME}"
                                echo "Using database: ${env.GR_DB_NAME}"
                            }

                            sh """
                            PGPASSWORD=${env.DB_PASSWORD} psql -h ${env.DB_POSTGRESQL_HOST} -U ${env.DB_USER} -d postgres <<EOF
                            CREATE DATABASE "${env.DB_NAME}" WITH TEMPLATE = template0 ENCODING = 'UTF8';
                            EOF
                            """
                            sh """
                            PGPASSWORD=${env.DB_PASSWORD} psql -h ${env.DB_POSTGRESQL_HOST} -U ${env.DB_USER} -d postgres <<EOF
                            CREATE DATABASE "${env.GRLIQ_DB_NAME}" WITH TEMPLATE = template0 ENCODING = 'UTF8';
                            EOF
                            """
                            sh """
                            PGPASSWORD=${env.DB_PASSWORD} psql -h ${env.DB_POSTGRESQL_HOST} -U ${env.DB_USER} -d postgres <<EOF
                            CREATE DATABASE "${env.GR_DB_NAME}" WITH TEMPLATE = template0 ENCODING = 'UTF8';
                            EOF
                            """
                            sh """
                            mysql -h ${env.DB_MARIA_HOST} -u ${env.DB_USER} -p${env.DB_PASSWORD} --ssl=0 -e 'CREATE DATABASE `${env.SPECTRUM_DB_NAME}`;'
                            """

                            script {
                                env.REDIS_DB = new Random().nextInt(1024).toString()
                                env.REDIS = "redis://${env.REDIS}:6379/${env.REDIS_DB}"
                                env.THL_REDIS = "redis://${env.THLREDIS}:6379/${env.REDIS_DB}"
                                echo "Using THL Redis: ${env.REDIS}"
                                if (sh(script: "redis-cli -u ${env.REDIS} SET jenkins_lock 1 NX EX 3600", returnStdout: true).trim() != 'OK')
                                    error('Redis already locked... aborting.')
                            }
                            script {
                                env.GR_REDIS_DB = new Random().nextInt(1024).toString()
                                env.GR_REDIS = "redis://${env.REDIS}:6379/${env.GR_REDIS_DB}"
                                echo "Using GR Redis: ${env.GR_REDIS}"
                                if (sh(script: "redis-cli -u ${env.GR_REDIS} SET jenkins_lock 1 NX EX 3600", returnStdout: true).trim() != 'OK')
                                    error('Redis already locked... aborting.')
                            }
                        }
                    }

                    stage('Setup Git') {
                        steps {
                            cleanWs()

                            dir('tmp') {
                                sh 'pwd -P'
                            }

                            dir("py-utils:$PYTHON_VERSION/") {
                                checkout scmGit(
                                    branches: [[name: env.BRANCH_NAME]],
                                    extensions: [ cloneOption(shallow: true) ],
                                    userRemoteConfigs: [
                                        [credentialsId:  'abdeb570-b708-44f3-b857-8a6b06ed9822',
                                         url: 'ssh://code.g-r-l.com:6611/py-utils']
                                    ],
                                )
                            }

                            dir("thl-spectrum:$PYTHON_VERSION/") {
                                checkout scmGit(
                                    branches: [[name: env.BRANCH_NAME]],
                                    extensions: [ cloneOption(shallow: true) ],
                                    userRemoteConfigs: [
                                        [credentialsId:  'abdeb570-b708-44f3-b857-8a6b06ed9822',
                                         url: 'ssh://code.g-r-l.com:6611/thl-marketplaces/thl-spectrum']
                                    ],
                                )
                            }

                            dir("grliq:$PYTHON_VERSION/") {
                                checkout scmGit(
                                    branches: [[name: env.BRANCH_NAME]],
                                    extensions: [ cloneOption(shallow: true) ],
                                    userRemoteConfigs: [
                                        [credentialsId:  'abdeb570-b708-44f3-b857-8a6b06ed9822',
                                         url: 'ssh://code.g-r-l.com:6611/grl-iq']
                                    ],
                                )
                            }

                            dir("gr:$PYTHON_VERSION/") {
                                checkout scmGit(
                                    branches: [[name: env.BRANCH_NAME]],
                                    extensions: [ cloneOption(shallow: true) ],
                                    userRemoteConfigs: [
                                        [credentialsId:  'abdeb570-b708-44f3-b857-8a6b06ed9822',
                                         url: 'ssh://code.g-r-l.com:6611/general-research/gr-carer']
                                    ],
                                )
                            }
                        }
                    }

                    stage('Env & Migration') {
                        steps {
                            dir("py-utils:$PYTHON_VERSION/") {
                                sh "/usr/local/bin/$PYTHON_VERSION -m venv $VENV-$PYTHON_VERSION"
                                sh "$VENV-$PYTHON_VERSION/bin/pip install -U setuptools wheel pip"
                                sh "$VENV-$PYTHON_VERSION/bin/pip install -r requirements.txt"
                                sh "$VENV-$PYTHON_VERSION/bin/pip install '.[django]'"
                                sh """
                                export DB_NAME=${DB_NAME}
                                export DB_USER=${env.DB_USER}
                                export DB_PASSWORD=${env.DB_PASSWORD}
                                export DB_HOST=${env.DB_POSTGRESQL_HOST}
                                $VENV-$PYTHON_VERSION/bin/$PYTHON_VERSION -m generalresearch.thl_django.app.manage migrate
                                """
                            }

                            dir("thl-spectrum:$PYTHON_VERSION/") {
                                dir('carer') {
                                    sh "/usr/local/bin/$PYTHON_VERSION -m venv $SPECTRUM_CARER_VENV-$PYTHON_VERSION"
                                    sh "$SPECTRUM_CARER_VENV-$PYTHON_VERSION/bin/pip install -U setuptools wheel pip"
                                    sh "$SPECTRUM_CARER_VENV-$PYTHON_VERSION/bin/pip install -r requirements.txt"

                                    sh """
                                        export DB_NAME=${SPECTRUM_DB_NAME}
                                        $SPECTRUM_CARER_VENV-$PYTHON_VERSION/bin/$PYTHON_VERSION manage.py migrate --settings=carer.settings.unittest
                                    """
                                }
                            }

                            dir("grliq:$PYTHON_VERSION/") {
                                dir('carer') {
                                    sh "/usr/local/bin/$PYTHON_VERSION -m venv $GRLIQ_CARER_VENV-$PYTHON_VERSION"
                                    sh "$GRLIQ_CARER_VENV-$PYTHON_VERSION/bin/pip install -U setuptools wheel pip"
                                    sh "$GRLIQ_CARER_VENV-$PYTHON_VERSION/bin/pip install -r requirements.txt"

                                    sh """
                                        export DB_NAME=${GRLIQ_DB_NAME}
                                        $GRLIQ_CARER_VENV-$PYTHON_VERSION/bin/$PYTHON_VERSION manage.py migrate --settings=carer.settings.unittest
                                    """
                                }
                            }

                            dir("gr:$PYTHON_VERSION/") {
                                sh "/usr/local/bin/$PYTHON_VERSION -m venv $GR_CARER_VENV-$PYTHON_VERSION"
                                sh "$GR_CARER_VENV-$PYTHON_VERSION/bin/pip install -U setuptools wheel pip"
                                sh "$GR_CARER_VENV-$PYTHON_VERSION/bin/pip install -r requirements.txt"

                                sh """
                                    export DB_NAME=${GR_DB_NAME}
                                    $GR_CARER_VENV-$PYTHON_VERSION/bin/$PYTHON_VERSION manage.py migrate --settings=gr.settings.unittest
                                """
                            }
                        }
                    }

                    stage('base') {
                        when {
                            expression { return true }
                        }
                        steps {
                            dir("py-utils:$PYTHON_VERSION") {
                                sh "$VENV-$PYTHON_VERSION/bin/pytest -v tests/sql_helper.py"
                            }
                        }
                    }

                    stage('models') {
                        when {
                            expression { return true }
                        }
                        steps {
                            dir("py-utils:$PYTHON_VERSION") {
                                sh "$VENV-$PYTHON_VERSION/bin/pytest -v tests/models"
                            }
                        }
                    }

                    stage('managers') {
                        steps {
                            dir("py-utils:$PYTHON_VERSION") {
                                sh "$VENV-$PYTHON_VERSION/bin/pytest -v tests/managers"
                            }
                        }
                    }

                    stage('wall_status_codes') {
                        steps {
                            dir("py-utils:$PYTHON_VERSION") {
                                sh "$VENV-$PYTHON_VERSION/bin/pytest -v tests/wall_status_codes"
                            }
                        }
                    }

                    stage('wxet') {
                        steps {
                            dir("py-utils:$PYTHON_VERSION") {
                                sh "$VENV-$PYTHON_VERSION/bin/pytest -v tests/wxet"
                            }
                        }
                    }

                    stage('grliq') {
                        steps {
                            dir("py-utils:$PYTHON_VERSION") {
                                sh "$VENV-$PYTHON_VERSION/bin/pytest -v tests/grliq"
                            }
                        }
                    }

                    stage('incite') {
                        steps {
                            dir("py-utils:$PYTHON_VERSION") {
                                sh "$VENV-$PYTHON_VERSION/bin/pytest -v tests/incite"
                            }
                        }
                    }
                }
            }
        }
    }
    post {
        always {
            echo 'One way or another, I have finished'
            deleteDir() /* clean up our workspace */
            sh """
            mariadb -h ${env.DB_MARIA_HOST} -u ${env.DB_USER} -p${env.DB_PASSWORD} --ssl=0 -e 'DROP DATABASE `${env.SPECTRUM_DB_NAME}`;'
            """
            sh """
            PGPASSWORD=${env.DB_PASSWORD} psql -h ${env.DB_POSTGRESQL_HOST} -U ${env.DB_USER} -d postgres <<EOF
            DROP DATABASE "${env.DB_NAME}";
            EOF
            """
            sh """
            PGPASSWORD=${env.DB_PASSWORD} psql -h ${env.DB_POSTGRESQL_HOST} -U ${env.DB_USER} -d postgres <<EOF
            DROP DATABASE "${env.GRLIQ_DB_NAME}";
            EOF
            """
            sh """
            PGPASSWORD=${env.DB_PASSWORD} psql -h ${env.DB_POSTGRESQL_HOST} -U ${env.DB_USER} -d postgres <<EOF
            DROP DATABASE "${env.GR_DB_NAME}";
            EOF
            """

            sh "redis-cli -u ${env.THL_REDIS} FLUSHDB"
            sh "redis-cli -u ${env.GR_REDIS} FLUSHDB"
        }
    }
}
