#!/usr/bin/env groovy

@Library("ci-library-versions") l1

node("spark-python39") {
    try {
        stage("Checkout") {
            checkout scm
        }
        stage("Load Libraries") {
            libraryversions("load")
        }
        stage("Set Libraries") {
            library "osiris-python-module@${OSIRIS_PYTHON_MODULE_VERSION}"
            library "osiris-moma-module@${OSIRIS_MOMA_MODULE_VERSION}"
            library "sonar@${SONAR_VERSION}"
        }

        stage("Python Worker") {
            if (!python.isBuildable()) {
                return true
            }

            def isNotebook = fileExists '.notebook'
            def isMoma = fileExists '.moma'
            def isDataflow = fileExists 'joysticksecuritizatiotl3swp/dataflow.py'
            def adapter = isNotebook ? python.&notebook : python.&worker;

            // Some adapter methods could not exist, in this case Osiris will print
            // a log and then the pipeline continues executing the next line.

            if (isNotebook || isMoma || isDataflow) {
                moma.developToMaster()
            }

            python("samuelPreBuild")
            adapter("verify")
            python("venv")
            adapter("requirementsAdd")
            adapter("export",["app.py","notebook31"])
            
            //On develop or non-master PRs, just package, don't build container and test
            if ((env.BRANCH_NAME.startsWith('PR-') && env.CHANGE_TARGET != 'master') ||
            (env.BRANCH_NAME == 'develop')) {
                python.worker('package')
            } else {
                adapter('containerBuild')
                adapter('containerSetup')

                parallel(
                    containerTest: {
                        adapter('containerTest')
                    },
                    containerPackage: {
                        adapter('containerPackage')
                    },
                    sonar: {
                        python('sonar')
                    }
                )
            }

            python('samuelPreDeploy')
            adapter('deploy')
            python('release')
        }
    } finally {
        if (python.isBuildable(false)) {
            python('vTrack')
        }
        python('containerClean')
        cleanWs()
    }
}