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

# create mountpoint
mkdir mountpoint

# mount bucket
bin/circus examplebucket mountpoint -o _netdev,rw,allow_other

# list bucket contents
ls mountpoint

# unmount bucket
fusermount -u mountpoint
```

You can use `mount` command by creating symbolic link in `/usr/sbin`.

```bash
sudo ln -s /usr/sbin/mount.s3 /path/to/circus

mount -t s3 examplebucket /path/to/mountpoint -o _netdev,rw,allow_other
```

To mount your Amazon S3 bucket on system startup, add the following line to `/etc/fstab`:

```
/path/to/circus#examplebucket /path/to/mountpoint fuse _netdev,rw,allow_other 0 0
```

## Command Line Options

- `-o OPTIONS`

    The mount options that would be passed to the `mount` command.

- `--region REGION`

    The name of AWS Region where your Amazon S3 bucket is located. If not specified, Circus will automatically detect the location of you bucket.

- `--log_output PATH`

    The path to the file where errors should be logged. If the special value `STDOUT` or `STDERR` is used, the errors are sent to stdout or stderr instead. By default, logging is disabled.

- `--log_level LEVEL`

    The severity threshold of logging. You can give one of the following levels: `FATAL`, `ERROR`, `WARN` (default), `INFO`, and `DEBUG`. Only messages at that level or higher will be logged.

- `--cache TYPE[:OPTIONS]`

    Select cache driver you would like to be used for caching objects' attributes. By default, Circus is configured to use the `memory` store. The valid values are:

    - `file[:DIR_PATH]`

        uses the local file system to store data. `DIR_PATH` represents the path to the directory where the store files will be stored (default: `/tmp/cache`).

    - `memory[:MAX_SIZE]`

        keeps data in memory in the same Circus process. `MAX_SIZE` represents the bounded size (in megabytes) of the cache store (default: 32 MB).

    - `memcached:HOST:PORT[:HOST:PORT ...]`

        uses memcached server to provide a centralized cache. `HOST:PORT` represents the addresses for all memcached servers in your cluster (default: 'localhost:11211').

- `--cache_ttl NUMBER`

    The number of seconds for which objects' attributes should be cached. This value must be larger than zero. The default value is 300 (5 minutes).

- `--access_key_id STRING`
- `--secret_access_key KEY`

    The AWS credentials which is required to access your AWS resources. If not specified, the default credential profiles file (typically located at `~/.aws/credentials`) or Amazon EC2 instance profile credentials will be used. (**WARNING:** This is convenient but insecure. On some systems, your password becomes visible to system status programs such as `ps` that may be invoked by other users to display command lines. You should consider using environment variables or Amazon EC2 instance profiles instead.)
