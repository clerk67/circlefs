# Circus

FUSE-based file system backed by Amazon S3

> **WARNING:** Circus is still under the development! Be careful to use in your environment.

## Features

- **Unlimited storage capacity** provided with Amazon S3
- Support renaming of files and directories (including **non-empty directories**)
- Support caching object attributes to reduce requests and improve performance
- Support **external memcached cluster** as cache store to share data among multiple clients

## Requirements

- Linux (macOS is not supported)
- [Ruby](https://www.ruby-lang.org/) >= 2.4.0
- [Bundler](https://bundler.io/)
- [FUSE](http://fuse.sourceforge.net/)

Circus requires some permissions to access your AWS resources. The required AWS managed IAM policies are:

- **AmazonS3FullAccess**
- **AmazonCloudWatchReadOnlyAccess** (for getting statistics returned for `df` request)

The following is an example of minimum required IAM policy document:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLocation",
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::examplebucket"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:DeleteObject",
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::examplebucket/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:GetMetricData"
      ],
      "Resource": "*"
    }
  ]
}
```

## Getting Started

The following is an example provisioning procedure for [Amazon Linux 2](https://aws.amazon.com/amazon-linux-2/) running on Amazon EC2 instances:

```bash
# install ruby
sudo amazon-linux-extras install ruby2.4

# install dependencies
sudo yum install fuse fuse-devel gcc git rpm-build ruby-devel

# install bundler
gem install bundler --no-document

# check out this repository
git clone https://github.com/clerk67/circus && cd circus

# install rubygems
bundle install

# configure circus
vim config.yml

# create mountpoint
mkdir mountpoint

# mount bucket (as a background process)
bin/circus mountpoint &

# list bucket contents
ls mountpoint
```
