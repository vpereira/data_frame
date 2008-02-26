Rake::TaskManager.class_eval do
  def remove_task(task_name)
    @tasks.delete(task_name.to_s)
  end
end
 
def remove_task(task_name)
  Rake.application.remove_task(task_name)
end
 
# Override existing test task to prevent integrations
# from being run unless specifically asked for
remove_task :release
desc 'Package and upload the release to Dolores Labs.'
task :release => [:clean, :package] do |t|
  v = ENV["VERSION"] or abort "Must supply VERSION=x.y.z"
  abort "Versions don't match #{v} vs #{VERS}" if v != VERS
  gem = "pkg/#{GEM_NAME}-#{VERS}.gem"

  `scp #{gem} deployer@mulva:/var/www/gems/gems`
  puts `ssh deployer@mulva gem generate_index -d /var/www/gems`
end