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
    }
    post {
        always {
            script { slack.currentResult(this, ":pipeline: ${SYSTEM_NAME} pipeline finished") }
        }
    }
}
