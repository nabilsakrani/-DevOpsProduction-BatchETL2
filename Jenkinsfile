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
        sh 'sudo cp target/*/*.jar /opt/deploy/batchETL'
        sh 'sudo cp conf/* /opt/deploy/batchETL/'
        sh 'sudo cp target/*/*.jar /opt/staging/IntegrationStagingProject/lib'
      }
    }
    stage('Integration Tests') {
      steps {
          sh 'cd /opt/staging/IntegrationStagingProject/ && sbt clean test'
      }
    }
    stage('Production Deploy') {
      steps {
        echo 'Safe to Deploy'
      }
    }
  }
}