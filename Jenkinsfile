pipeline {
    agent any
    stages {
        stage('Test') {
            steps {
                sh '''
                cd /tmp
                wget https://dl.google.com/go/go1.13.3.linux-amd64.tar.gz -c
                tar -xzf go1.13.3.linux-amd64.tar.gz
                '''
                timeout(time: 5, unit: 'MINUTES') {
                    sh '''
                    export GOROOT=/tmp/go
                    export PATH=$GOPATH/bin:$GOROOT/bin:$PATH
                    go get github.com/m-motawea/pipeline
                    cd ~/go/src/github.com/m-motawea/pipeline
                    go test -v
                    '''   
                }
            }
        }
    }
}
