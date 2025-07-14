#!/usr/bin/env groovy

@Library(["osiris-python-module@release/python39-ada", "osiris-k8s-module"]) _
adanode("sagemaker-py39-sp33") {
    try {
        def integrationbranch = "feat/cast_facilities"
        stage('Checkout') {
            checkout([$class: 'GitSCM',
                branches: [[name: "*/${integrationbranch}"]],
                userRemoteConfigs: [[url: scm.userRemoteConfigs[0].url]]
            ])
        }
        env.AWS_DEFAULT_REGION = "eu-south-2"
        if (python.isBuildable()) {
            python.sagemaker("pipeline")
        }

    } finally {
        cleanWs()
    }
}