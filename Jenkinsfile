pipeline {
  agent any
  stages {
    stage('Setup Env') {
      steps {
        echo 'Setup the system'
        echo 'wget, curl, java, sbt and spark are now installed by Config Management system :)'
      }
    }
    stage('Env setup test') {
      steps {
        sh 'java -version'
        sh 'sbt about'
      }
    }
    stage('Unit Tests') {
      steps {
        sh 'sbt clean coverage test coverageReport'
        archiveArtifacts 'target/test-reports/*.xml'
        archiveArtifacts 'target/scala-2.11/scoverage-report/*'
      }
    }
    stage('Build') {
      steps {
        sh 'sbt clean compile package assembly'
        archiveArtifacts(artifacts: 'target/scala-*/*.jar', fingerprint: true)
      }
    }
    stage('Staging Deploy') {
      steps {
        sh 'sudo cp target/*/*assembly*.jar /opt/deploy/batch_etl'
        sh 'sudo cp conf/* /opt/deploy/batchETL/'
        sh 'sudo cp target/*/*assembly*.jar /opt/staging/IntegrationStagingProject/lib'
      }
    }
    stage('Integration Tests') {
      steps {
        sh 'cd /opt/staging/IntegrationStagingProject/ && sbt clean test'
        archiveArtifacts 'target/test-reports/*.xml'
      }
    }
    stage('Production Deploy') {
      steps {
        echo 'Safe to Deploy in Production, Great Job :D'
        sh 'ansible-playbook -i \'worker-test,\' --private-key=/home/xxpasquxx/.ssh/ansible_rsa_key /opt/DevOpsProduction-Orchestrator/ansible/deploy/batch_etl_deploy.yml  -e \'ansible_ssh_user=xxpasquxx\' -e \'host_key_checking=False\''
      }
    }
  }
  post {
    always {
      echo 'One way or another, I have finished'
    }

    success {
      script {
        header = "Job <${env.JOB_URL}|${env.BRANCH_NAME}> <${env.JOB_DISPLAY_URL}|(Blue)>"
        header += " build <${env.BUILD_URL}|${env.BUILD_DISPLAY_NAME}> <${env.RUN_DISPLAY_URL}|(Blue)>:"
        message = "${header}\nsuccessful :smiley:"

        author = sh(script: "git log -1 --pretty=%an", returnStdout: true).trim()
        commitMessage = sh(script: "git log -1 --pretty=%B", returnStdout: true).trim()
        message += " Commit by <@${author}> (${author}): ``` ${commitMessage} ``` "
        color = '#00CC00'
      }

      echo "Message ${message}"
      slackSend(message: message, baseUrl: 'https://devops-pasquali-cm.slack.com/services/hooks/jenkins-ci/', color: color, token: 'ihoCVUPB7hqGz2xI1htD8x0F')

    }

    unstable {
      script {
        header = "Job <${env.JOB_URL}|${env.BRANCH_NAME}> <${env.JOB_DISPLAY_URL}|(Blue)>"
        header += " build <${env.BUILD_URL}|${env.BUILD_DISPLAY_NAME}> <${env.RUN_DISPLAY_URL}|(Blue)>:"
        message = "${header}\nsuccessful :smiley:"

        author = sh(script: "git log -1 --pretty=%an", returnStdout: true).trim()
        commitMessage = sh(script: "git log -1 --pretty=%B", returnStdout: true).trim()
        message += " Commit by <@${author}> (${author}): ``` ${commitMessage} ``` "
        color = '#00CC00'
      }

      echo "Message ${message}"
      slackSend(message: message, baseUrl: 'https://devops-pasquali-cm.slack.com/services/hooks/jenkins-ci/', color: color, token: 'ihoCVUPB7hqGz2xI1htD8x0F')

    }

    failure {
      script {
        header = "Job <${env.JOB_URL}|${env.BRANCH_NAME}> <${env.JOB_DISPLAY_URL}|(Blue)>"
        header += " build <${env.BUILD_URL}|${env.BUILD_DISPLAY_NAME}> <${env.RUN_DISPLAY_URL}|(Blue)>:"
        message = "${header}\nsFAILED :(:"

        author = sh(script: "git log -1 --pretty=%an", returnStdout: true).trim()
        commitMessage = sh(script: "git log -1 --pretty=%B", returnStdout: true).trim()
        message += " Commit by <@${author}> (${author}): ``` ${commitMessage} ``` "
        color = '#FFDD12'
      }

      echo "Message ${message}"
      slackSend(message: message, baseUrl: 'https://devops-pasquali-cm.slack.com/services/hooks/jenkins-ci/', color: color, token: 'ihoCVUPB7hqGz2xI1htD8x0F')

    }

  }
}