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
        stage('python versions') {

            matrix {
                axes {
                    axis {
                        name 'PYTHON_VERSION'
                        values 'python3.14' 'python3.13', 'python3.12', 'python3.11'
                    }
                }

                stages {

                    stage('Setup') {
                        steps {
                            cleanWs()

                            dir('tmp') {
                                sh 'pwd -P'
                            }

                            dir("generalresearch:$PYTHON_VERSION/") {
                                checkout scmGit(
                                    branches: [[name: env.BRANCH_NAME]],
                                    extensions: [ cloneOption(shallow: true) ],
                                    userRemoteConfigs: [
                                        [credentialsId:  'abdeb570-b708-44f3-b857-8a6b06ed9822',
                                         url: 'ssh://code.g-r-l.com:6611/generalresearch']
                                    ],
                                )

                                sh "/usr/local/bin/$PYTHON_VERSION -m venv $VENV-$PYTHON_VERSION"
                                sh "$VENV-$PYTHON_VERSION/bin/pip install -U setuptools wheel pip"
                                sh "$VENV-$PYTHON_VERSION/bin/pip install -r requirements.txt"
                                sh "$VENV-$PYTHON_VERSION/bin/pip install '.[django]'"
                            }
                        }
                    }

                    stage('base') {
                        steps {
                            dir("generalresearch:$PYTHON_VERSION") {
                                sh "$VENV-$PYTHON_VERSION/bin/pytest tests/models/gr/test_base.py -vs"
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
        }
    }
}
