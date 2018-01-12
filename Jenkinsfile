pipeline {
  agent any
  stages {
    stage('Config System') {
      steps {
        echo 'Setup the system'
        echo 'wget, curl, java, sbt and spark are now installed by Config Management system :)'
      }
    }
    stage('Test the System') {
      steps {
        sh 'java -version'
        sh 'sbt about'
      }
    }
    stage('Test scalatest') {
      steps {
        sh 'sbt clean test'
      }
    }
    stage('Build') {
      steps {
        sh 'sbt clean compile package assembly'
      }
    }
    stage('Deploy') {
      steps {
        sh 'sudo cp target/*/*.jar /opt/deploy/batchETL'
        sh 'sudo cp conf/* /opt/deploy/batchETL/'
      }
    }
  }
  post {
    always {
      archiveArtifacts artifacts: 'target/scala-*/*.jar', fingerprint: true
      archiveArtifacts 'target/test-reports/*.xml'
    }
  }
}