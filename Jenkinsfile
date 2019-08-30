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
        stage("Check if we should build") {
            steps {
                script {
                    RECENT_HISTORY = sh(script: "git log --since='4 days ago' --format=oneline origin/${GIT_BRANCH}", returnStdout: true).trim()
                    echo "${RECENT_HISTORY}"
                    if ("${RECENT_HISTORY}" == "") {
                        githubNotify context:'Check Age', description:'Skipping build because this branch is too old, please merge or rebase', status: 'FAILURE'
                        error "Skipping build because the recent log is too old, please rebase or add new commits"
                    }
                    githubNotify context:'Check Age', status: 'SUCCESS', description:'This PR is new enough'
                }

            }
        }

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
                    branch 'patch-*';
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
                    sh "pip install .[results,doc,test]"
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
    }
    post {
        always {
            script { slack.currentResult(this, ":pipeline: ${SYSTEM_NAME} pipeline finished") }
        }
    }
}
