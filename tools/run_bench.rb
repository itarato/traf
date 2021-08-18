def parse_out(out)
  # Example:
  # "0.00user 0.00system 0:00.00elapsed 150%CPU (0avgtext+0avgdata 4088maxresident)k\n0inputs+0outputs (0major+206minor)pagefaults 0swaps\n"

  parts = out.split(' ')

  raise unless parts[0].end_with?('user')
  raise unless parts[1].end_with?('system')
  raise unless parts[2].end_with?('elapsed')

  t_user = parts[0][..-5]
  t_system = parts[1][..-7]
  t_elapsed = parts[2][..-8]

  [
    t_user, 
    t_system, 
    t_elapsed, 
    out,
  ]
end

def run(concurrency = 1, iterations = 1)
  parse_out(`time /mnt/data/Code/Rust/traf/traf_benchmark/target/release/traf_benchmark -c #{concurrency} -i #{iterations} 2>&1`)
end

p(run(1, 1000))
p(run(10, 100))
p(run(100, 10))
p(run(1000, 1))
