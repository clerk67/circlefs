require 'optparse'
require 'rfusefs'

module CircusOptionParser

  def parse_options(argv)
    options = {
      :_netdev => true,
      :access_key_id => nil,
      :allow_other => true,
      :bucket => nil,
      :cache => 'memory',
      :cache_ttl => 300,
      :log_level => 'WARN',
      :log_output => nil,
      :region => nil,
      :secret_access_key => nil,
      :rw => true,
    }
    optval = []
    opt = OptionParser.new
    opt.on('-o OPTIONS', 'The mount options that would be passed to the mount command.') do |value|
      optval << value
    end
    opt.on('-r', '--region REGION', 'The name of AWS Region where your Amazon S3 bucket is located.') do |value|
      options[:region] = value
    end
    opt.on('-l', '--log_output PATH', 'The path to the file where errors should be logged (or STDOUT, STDERR).') do |value|
      options[:log_output] = value
    end
    opt.on('-v', '--log_level LEVEL', 'The severity threshold of logging (levels: FATAL, ERROR, WARN, INFO, DEBUG).') do |value|
      options[:log_level] = value
    end
    opt.on('-c', '--cache TYPE[:OPTIONS]', 'The cache driver you would like to be used for caching objects\' attributes.') do |value|
      options[:cache] = value
    end
    opt.on('-t', '--cache_ttl NUMBER', 'The number of seconds for which objects\' attributes should be cached.') do |value|
      options[:cache_ttl] = value
    end
    opt.on('--access_key_id STRING', 'The AWS access key ID which is required to access your AWS resources.') do |value|
      options[:access_key_id] = value
    end
    opt.on('--secret_access_key KEY', 'The AWS secret access key which is required to access your AWS resources.') do |value|
      options[:secret_access_key] = value
    end
    opt.banner << ' <bucket> <mountpoint>'
    opt.version = self::CIRCUS_VERSION
    opt.parse!(argv)

    mountpoint = argv[1]
    options[:bucket] = argv[0]
    unless mountpoint && options[:bucket] then
      STDERR.puts "ERROR: invalid arguments"
      STDERR.puts "To see help text, run: circus --help"
      exit!
    end
    options.each do |key, value|
      if value == true then
        optval << "#{key}"
      elsif value
        optval << "#{key}=#{value}"
      end
    end
    options = RFuse.parse_options(%W(#{mountpoint} -o #{optval.join(',')}), *options.keys)
    [options, mountpoint]
  end

end
