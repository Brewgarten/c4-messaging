pipeline {
  agent {
    docker {
      image "localhost:5000/centos-6-dynamite-python:2.2"
      label "docker"
    }
  }

  stages {
    stage("checkout") {
      steps {
        script {
          // determine if the current commit has a tag and adjust build name
          tagged = sh(returnStdout: true, script: "git describe --exact-match --tags HEAD &> /dev/null && echo true || echo false").trim().toBoolean()
          currentBuild.displayName = sh(returnStdout: true, script: "git describe --tags HEAD").trim()

          // make sure we re-attach HEAD
          latestCommit = sh(returnStdout: true, script: "git rev-parse HEAD").trim()
          sh "git checkout ${latestCommit}"
        }

        // create dependency folder
        sh "mkdir -p lib/c4"
      }
    }

    stage("retrieve dependencies") {
      steps {
        parallel (
          "c4-utils": {
            dir("lib/c4/utils") {
              git credentialsId: "github", url: "https://github.ibm.com/c4/utils"
            }
            sh 'cd lib/c4/utils && git checkout tags/$(git describe --abbrev=0 --tags)'
          }
        )
      }
    }

    stage("code analysis") {
      steps {
        // set up virtual environment
        sh "rm -rf env"
        sh "dynamite-python -m virtualenv --system-site-packages env"
        sh "env/bin/pip install --disable-pip-version-check --no-cache-dir lib/c4/utils"

        // run analysis
        sh "rm -rf pylint.log"
        sh "env/bin/dynamite-python -m pylint --version"
        // TODO: once the initial errors are fixed we can avoid hack to always succeed
        sh "env/bin/dynamite-python -m pylint --output-format=parseable c4 > pylint.log || echo 0"
      }
      post {
        always {
          // publish code analysis results
          step([$class: "WarningsPublisher",
            parserConfigurations: [[
              parserName: "PyLint",
              pattern: "pylint.log"
            ]]
          ])
          archive "pylint.log"
        }
      }
    }

    stage("unit test") {
      steps {
        // set up virtual environment
        sh "rm -rf env"
        sh "dynamite-python -m virtualenv --system-site-packages env"
        sh "env/bin/pip install --disable-pip-version-check --no-cache-dir lib/c4/utils"

        // run tests
        sh "rm -rf test_results && mkdir test_results"
        sh "env/bin/dynamite-python setup.py coverage"
      }
      post {
        always {
          // publish test results
          junit "test_results/test_results.xml"
          // publish coverage information
          publishHTML (target: [
            allowMissing: false,
            alwaysLinkToLastBuild: false,
            keepAll: true,
            reportDir: "test_results/coverage",
            reportFiles: "index.html",
            reportName: "Coverage Report"
          ])
        }
      }
    }

    stage("package") {
      steps {
        // start with fresh dist folder
        sh "rm -rf dist"
        sh "dynamite-python setup.py sdist"
        dir("dist") {
          archive includes: "*"
        }
      }
      when {
        // only package if we have a tag
        expression {
          return tagged
        }
      }
    }
  }
}