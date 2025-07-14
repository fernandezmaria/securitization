#!/usr/bin/env groovy

@Library(["osiris-python-module@release/python39-ada", "osiris-k8s-module"]) _
adanode("sagemaker-py39-sp33") {
    try {
        stage('Checkout') {
            checkout scm
        }
        env.AWS_DEFAULT_REGION = "eu-south-2"
        if (python.isBuildable()) {
            python.sagemaker("pipeline")
        }

    } finally {
        cleanWs()
    }
}