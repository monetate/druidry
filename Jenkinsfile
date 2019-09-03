#!/usr/bin/env groovy

REPO_NAME = 'druidry'
SYSTEM_NAME = 'druidry'
VENV = 'virtualenv'

@Library('monetate-jenkins-pipeline')
import org.monetate.Slack
def slack = new Slack(steps, REPO_NAME)

pipeline {
    agent { label "node" }
    environment {
        TMPDIR='/var/lib/jenkins/tmp'
        PYTHONUNBUFFERED=1
    }
    options {
        timestamps()
    }
    stages {
        stage("Slack start message") {
            steps {
                script { slack.success(this, ":pipeline: ${SYSTEM_NAME} pipeline started") }
            }
        }

        stage('Checkout source') {
            steps {
                checkout scm
            }
        }

        stage ('Clean') {
            when {
                anyOf {
                    branch 'master';
                    changelog '.*clean.*';
                }
            }
            steps {
                sh "rm -rf ${WORKSPACE}/venv"
            }
        }

        stage('Install requirements') {
            steps {
                sh "${VENV} ${WORKSPACE}/venv"
                withEnv(["PATH=${env.WORKSPACE}/venv/bin:${env.PATH}"]) {
                    githubNotify context:'Python Requirements', description:'Installing python requirements',  status: 'PENDING'
                    sh "pip install -e .[results,doc,test]" // install editable so it doesn't build a cached wheel
                }
            }
            post {
                success {
                    githubNotify context:'Python Requirements', status: 'SUCCESS', description:'pip install succeeded'
                }
                failure {
                    githubNotify context:'Python Requirements', description:'Failed installing pip requirements', status: 'FAILURE'
                }
            }
        }

        stage ('Run Tests') {
            steps {
                withEnv(["PATH=${env.WORKSPACE}/venv/bin:${env.PATH}"]) {
                    githubNotify context:'Run Tests', status: 'PENDING', description:'running tests'
                    sh "python -m unittest tests"
                }
            }
            post {
                success {
                    githubNotify context:'Run Tests', status: 'SUCCESS', description:'passed all tests'
                }
                failure {
                    githubNotify context:'Run Tests', description:'Failed tests', status: 'FAILURE'
                }
            }
        }

        stage ('Auto-build wheel on master') {
            //when {
            //    branch 'master';
            //}
            steps {
                withEnv(["PATH=${env.WORKSPACE}/venv/bin:${env.PATH}"]) {
                    script {
                        VERSION_STRING = sh(script: "cat src/druidry/VERSION", returnStdout: true).trim()
                        echo "${VERSION_STRING}"
                        VERSION_LIST = sh(script: "echo ${VERSION_STRING} | tr '.' ' '", returnStdout: true).trim()
                        echo "${VERSION_LIST}"
                        V_MAJOR = sh(script: "${VERSION_LIST[0]}", returnStdout: true).trim()
                        V_MINOR = sh(script: "${VERSION_LIST[1]}", returnStdout: true).trim()
                        V_MICRO = sh(script: "${VERSION_LIST[2]}", returnStdout: true).trim()
                        echo "${V_MAJOR} ${V_MINOR} ${V_MICRO}"
                        V_MICRO_NEW = sh(script: "expr ${V_MICRO} + 1", returnStdout: true).trim()
                        NEW_VERSION_STRING = sh(script: "${V_MAJOR}.${V_MINOR}.${V_MICRO}", returnStdout: true).trim()
                        sh "echo ${NEW_VERSION_STRING} > src/druidry/VERSION"
                        sh "git add sry/druidry/VERSION"
                        sh "git commit -m 'Bump micro version for master commit'"
                        sh "git tag -a -m 'Tag version ${NEW_VERSION_STRING}' ${NEW_VERSION_STRING}"
                        sh "git push origin --tags"
                    }

                    sh "rm -rf ${WORKSPACE}/dist/"
                    sh "python setup.py bdist_wheel --universal"
                    sh "pip install twine"
                    //sh "twine upload --repository local ${WORKSPACE}/dist/*.whl"
                }
            }
        }
    }
    post {
        always {
            script { slack.currentResult(this, ":pipeline: ${SYSTEM_NAME} pipeline finished") }
        }
    }
}
