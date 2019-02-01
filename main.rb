require 'bundler/setup'
require 'aws-sdk-s3'
require 'rfusefs'

class CircleFS

  def initialize(options)
    @client = Aws::S3::Client.new(region: options[:region])
    @bucket = options[:bucket]
  end

  def contents(path)
    resp = @client.list_objects_v2({
      bucket: @bucket,
      delimiter: '/',
      prefix: path.sub(/^\//, '')
    })
    files = resp.contents.map do |item|
      item.key
    end
    directories = resp.common_prefixes.map do |item|
      item.prefix.sub(/\/$/, '')
    end
    files + directories
  end

  def file?(path)
    path == '/index.html'
  end

  def read_file(path)
    resp = @client.get_object({
      bucket: @bucket,
      key: path.sub(/^\//, '')
    })
    resp.body.read
  end

end

FuseFS.main(ARGV, [:bucket, :region]) do |options|
  CircleFS.new(options)
end
