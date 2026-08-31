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
        VENV = "${env.WORKSPACE}/generalresearch-venv"
    }

    stages {

        stage('Checkout') {
            steps {
                checkout scmGit(
                    branches: [[name: "*/${env.BRANCH_NAME}"]],
                    extensions: [ cloneOption(shallow: true) ],
                    userRemoteConfigs: [
                        [credentialsId: 'abdeb570-b708-44f3-b857-8a6b06ed9822',
                         url: 'ssh://code.g-r-l.com:6611/generalresearch']
                    ],
                )
                stash name: 'source', useDefaultExcludes: false
            }
        }

        stage('Python Versions') {
            matrix {
                axes {
                    axis {
                        name 'VER'
                        values 'python3.14', 'python3.13', 'python3.12'
                    }
                }

                stages {
                    stage('Setup') {
                        steps {
                            dir("generalresearch-${VER}") {
                                deleteDir()
                                unstash 'source'

                                withCredentials([file(
                                    credentialsId: '971e1f48-09ce-4446-9155-a52c1adb6249',
                                    variable: 'ENV_TEST_FILE')]) {
                                    sh 'cp $ENV_TEST_FILE .env.test'
                                }
                                sh "/usr/local/bin/${VER} -m venv ${VENV}-${VER}"
                                sh "${VENV}-${VER}/bin/pip install -U setuptools wheel pip"
                                sh "${VENV}-${VER}/bin/pip install '.'"
                                sh "${VENV}-${VER}/bin/pip install '.[django,dask]'"
                            }
                        }
                    }

                    stage('base') {
                        steps {
                            dir("generalresearch-${VER}") {
                                sh "${VENV}-${VER}/bin/pytest tests/models/gr/test_base.py -vs"
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
            deleteDir()
        }
    }
}