provider "aws" {
  region  = "us-east-1"
}

locals {
  subnet_id = "subnet-0bf671b4bcc2d80c8"
  vpc_id = "sg-033835348406c7269"
  key_name = "mark-chen-IAM-key-pair"
  ami = "ami-3508e54f"
  num_brokers = 1
  num_ksqls = 1
}

resource "aws_instance" "brokers" {
  count = local.num_brokers

  ami = local.ami
  instance_type = "m4.large"
  key_name = local.key_name
  vpc_security_group_ids = [local.vpc_id]
  subnet_id = local.subnet_id

  tags = {
    Name = "brokers"
    Index = "Server ${count.index}"
    Role = "master"
    Owner = "a67"
  }

  root_block_device {
    volume_size = 20
  }

  provisioner "file" {
    source      = "."
    destination = "/home/ubuntu/heart_watch/"
  }

  provisioner "local-exec" {
    command = "bash ~/heart_watch/setup/stage2/session1.sh"
  }

  provisioner "local-exec" {
    command = "pip3 install -r requirements.txt"
    working_dir = "/home/ubuntu/heart_watch"
  }
}

# resource "aws_instance" "noncore" {
#   ami = local.ami
#   instance_type = "m4.large"
#   key_name = local.key_name
#   vpc_security_group_ids = [local.vpc_id]
#   subnet_id = local.subnet_id
#   associate_public_ip_address = true

#   tags = {
#     Name = "noncore"
#     Role = "master"
#     Owner = "a67"
#   }

#   root_block_device {
#     volume_size = 20
#   }
# }

# resource "aws_instance" "ksqls" {
#   count = local.num_ksqls

#   ami = local.ami
#   instance_type = "m4.large"
#   key_name = local.key_name
#   vpc_security_group_ids = [local.vpc_id]
#   subnet_id = local.subnet_id
#   associate_public_ip_address = true

#   tags = {
#     Name = "ksql"
#     Role = "master"
#     Owner = "a67"
#   }

#   root_block_device {
#     volume_size = 20
#   }
# }

resource "aws_eip" "brokers_eip" {
  count = local.num_brokers

  instance = "${aws_instance.brokers[count.index].id}"
  vpc      = true
}

# resource "aws_eip" "noncore_eip" {
#   instance = "${aws_instance.noncore.id}"
#   vpc      = true
# }

# resource "aws_eip" "ksqls_eip" {
#   count = local.num_ksqls

#   instance = "${aws_instance.ksqls[count.index].id}"
#   vpc      = true
# }


