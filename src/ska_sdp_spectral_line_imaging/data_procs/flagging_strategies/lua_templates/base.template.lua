aoflagger.require_min_version("3.0")
function execute(input)
  {setup_template}
  {original_mask}
  local copy_of_input = input:copy()
  {iteration_template}
  {scale_invarient}
  aoflagger.threshold_timestep_rms(input, {threshold_timestep_rms})
  if input:is_complex() and input:has_metadata() then
    aoflagger.collect_statistics(input, copy_of_input)
  end
  input:flag_nans()
end