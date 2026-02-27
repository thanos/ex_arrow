import Config

# In dev/test, build the NIF from source so `mix compile` and `mix ci` work without
# precompiled artifacts or EX_ARROW_BUILD. Release consumers use precompiled NIFs.
if config_env() in [:dev, :test] do
  config :rustler_precompiled, :force_build, ex_arrow: true
  import_config "#{config_env()}.exs"
end
