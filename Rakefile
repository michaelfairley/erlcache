def run(command)
  raise  unless system(command)
end

def with_erlcache(&blk)
  run("./rel/dummynode/bin/dummynode start")
  sleep 1
  yield
ensure
  run("./rel/dummynode/bin/dummynode ping || ./rel/dummynode/bin/dummynode stop")
end

task :build do
  run("rebar compile generate")
end

task :memcached_test => [:build] do
  with_erlcache do
    run("python integration_tests/memcached-test/testClient.py")
  end
end

task :default => [:memcached_test]
