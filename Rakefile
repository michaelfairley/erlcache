def run(command)
  raise  unless system(command)
end

def with_erlcache(&blk)
  run("./rel/dummynode/bin/dummynode start")
  sleep 1
  yield
ensure
  run("./rel/dummynode/bin/dummynode stop")
end

task :compile do
  run("rebar compile")
end

task :build_release => [:compile] do
  run("rebar generate")
end

task :memcached_test => [:build_release] do
  with_erlcache do
    run("python integration_tests/memcached-test/testClient.py")
  end
end

task :default => [:memcached_test]
