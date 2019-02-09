require_relative '../lib/circus.rb'

describe Circus do
  bucket = 'examplebucket'
  mountpoint = File.expand_path('../../mountpoint', __FILE__)

  it 'should list bucket contents' do
    argv = %W(#{bucket} #{mountpoint} -o _netdev,rw,allow_other)
    options, mountpoint = Circus.parse_options(argv)
    Circus.mount(Circus.new(options), mountpoint)

    sleep 1
    contents = Dir.entries(mountpoint)
    expect(contents).to include 'index.html'

    Circus.unmount(mountpoint)
  end
end
