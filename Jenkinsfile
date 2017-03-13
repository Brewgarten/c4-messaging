pipeline {
  agent {
    dockerfile {
      filename "ci-environment.Dockerfile"
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
        sh "mkdir -p lib"
      }
    }

    stage("retrieve dependencies") {
      steps {
        parallel (
          "utils": {
            dir("lib/utils") {
              git credentialsId: "github", url: "https://github.ibm.com/c4/utils"
              script {
                latestTag = sh(returnStdout: true, script: "git describe --abbrev=0 --tags").trim()
              }
              sh "git checkout tags/" + latestTag
            }
          }
        )
      }
    }

    stage("code analysis") {
      steps {
        // set up virtual environment
        sh "rm -rf env"
        sh "python -m virtualenv --system-site-packages env"
        sh "env/bin/pip install --disable-pip-version-check --no-cache-dir lib/utils"

        // run analysis
        sh "rm -rf pylint.log"
        sh "python -m pylint --version"
        // TODO: once the initial errors are fixed we can avoid hack to always succeed
        sh "env/bin/python -m pylint --output-format=parseable c4 > pylint.log || echo 0"
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
        sh "python -m virtualenv --system-site-packages env"
        sh "env/bin/pip install --disable-pip-version-check --no-cache-dir lib/utils"

        // run tests
        sh "rm -rf test_results && mkdir test_results"
        sh "env/bin/python setup.py coverage"
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
        sh "python setup.py sdist"
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