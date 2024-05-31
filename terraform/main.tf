terraform {
    backend "s3"{
        bucket = "sourcess-test-sate"
        key = "terraform.tfstate"
        region = "eu-west-2"
    }
}    

provider "aws"{
        region = "eu-west-2"
}

