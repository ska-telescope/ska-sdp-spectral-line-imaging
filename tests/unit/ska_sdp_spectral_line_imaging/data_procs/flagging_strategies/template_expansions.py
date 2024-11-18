default_all_true = """aoflagger.require_min_version("3.0")
function execute(input)
  local flag_polarizations = input:get_polarizations()
  local base_threshold = 2.0
  local flag_representations = { "amplitude" }
  local iteration_count = 3
  local threshold_factor_step = 4.0
  local transient_threshold_factor = 5.0
  local copy_of_input = input:copy()
  for ipol, polarization in ipairs(flag_polarizations) do
    local pol_data = input:convert_to_polarization(polarization)
    local converted_data
    local converted_copy
    for _, representation in ipairs(flag_representations) do
      converted_data = pol_data:convert_to_complex(representation)
      converted_copy = converted_data:copy()
      for i = 1, iteration_count - 1 do
        local threshold_factor = threshold_factor_step ^ (iteration_count - i)
        local sumthr_level = threshold_factor * base_threshold
        aoflagger.sumthreshold_masked(
          converted_data,
          converted_copy,
          sumthr_level,
          sumthr_level * transient_threshold_factor,
          true,
          true
        )
        local chdata = converted_data:copy()
        aoflagger.threshold_timestep_rms(converted_data, 3.0)
        aoflagger.threshold_channel_rms(chdata, 3.0 * threshold_factor, true)
        converted_data:join_mask(chdata)
        converted_data:set_visibilities(converted_copy)
        converted_data:join_mask(converted_copy)
        aoflagger.low_pass_filter(converted_data, 5, 3, 6.0, 7.0)
        local tmp = converted_copy - converted_data
        tmp:set_mask(converted_data)
        converted_data = tmp
        aoflagger.set_progress(
          (ipol - 1) * iteration_count + i,
          #flag_polarizations * iteration_count
        )
      end
      aoflagger.sumthreshold_masked(
        converted_data,
        converted_copy,
        base_threshold,
        base_threshold * transient_threshold_factor,
        true,
        true
      )
    end
    converted_data:join_mask(converted_copy)
    aoflagger.set_progress(ipol, #flag_polarizations)
  end
  aoflagger.scale_invariant_rank_operator_masked(
    input, copy_of_input, 0.2, 0.2
  )
  aoflagger.threshold_timestep_rms(input, 3.0)
  if input:is_complex() and input:has_metadata() then
    aoflagger.collect_statistics(input, copy_of_input)
  end
  input:flag_nans()
end"""

default_no_flag_low_outliers = """aoflagger.require_min_version("3.0")
function execute(input)
  local flag_polarizations = input:get_polarizations()
  local base_threshold = 2.0
  local flag_representations = { "amplitude" }
  local iteration_count = 3
  local threshold_factor_step = 4.0
  local transient_threshold_factor = 5.0
  input:clear_mask()
  local copy_of_input = input:copy()
  for ipol, polarization in ipairs(flag_polarizations) do
    local pol_data = input:convert_to_polarization(polarization)
    local converted_data
    local converted_copy
    for _, representation in ipairs(flag_representations) do
      converted_data = pol_data:convert_to_complex(representation)
      converted_copy = converted_data:copy()
      for i = 1, iteration_count - 1 do
        local threshold_factor = threshold_factor_step ^ (iteration_count - i)
        local sumthr_level = threshold_factor * base_threshold
        aoflagger.sumthreshold(
          converted_data,
          sumthr_level,
          sumthr_level * transient_threshold_factor,
          true,
          true
        )
        local chdata = converted_data:copy()
        aoflagger.threshold_timestep_rms(converted_data, 3.0)
        aoflagger.threshold_channel_rms(chdata, 3.0 * threshold_factor, false)
        converted_data:join_mask(chdata)
        converted_data:set_visibilities(converted_copy)
        aoflagger.low_pass_filter(converted_data, 5, 3, 6.0, 7.0)
        local tmp = converted_copy - converted_data
        tmp:set_mask(converted_data)
        converted_data = tmp
        aoflagger.set_progress(
          (ipol - 1) * iteration_count + i,
          #flag_polarizations * iteration_count
        )
      end
      aoflagger.sumthreshold(
        converted_data,
        base_threshold,
        base_threshold * transient_threshold_factor,
        true,
        true
      )
    end
    aoflagger.set_progress(ipol, #flag_polarizations)
  end
  aoflagger.scale_invariant_rank_operator(
    input,  0.2, 0.2
  )
  aoflagger.threshold_timestep_rms(input, 3.0)
  if input:is_complex() and input:has_metadata() then
    aoflagger.collect_statistics(input, copy_of_input)
  end
  input:flag_nans()
end"""


default_no_original_mask = """aoflagger.require_min_version("3.0")
function execute(input)
  local flag_polarizations = input:get_polarizations()
  local base_threshold = 2.0
  local flag_representations = { "amplitude" }
  local iteration_count = 3
  local threshold_factor_step = 4.0
  local transient_threshold_factor = 5.0
  input:clear_mask()
  local copy_of_input = input:copy()
  for ipol, polarization in ipairs(flag_polarizations) do
    local pol_data = input:convert_to_polarization(polarization)
    local converted_data
    local converted_copy
    for _, representation in ipairs(flag_representations) do
      converted_data = pol_data:convert_to_complex(representation)
      converted_copy = converted_data:copy()
      for i = 1, iteration_count - 1 do
        local threshold_factor = threshold_factor_step ^ (iteration_count - i)
        local sumthr_level = threshold_factor * base_threshold
        aoflagger.sumthreshold(
          converted_data,
          sumthr_level,
          sumthr_level * transient_threshold_factor,
          true,
          true
        )
        local chdata = converted_data:copy()
        aoflagger.threshold_timestep_rms(converted_data, 3.0)
        aoflagger.threshold_channel_rms(chdata, 3.0 * threshold_factor, true)
        converted_data:join_mask(chdata)
        converted_data:set_visibilities(converted_copy)
        aoflagger.low_pass_filter(converted_data, 5, 3, 6.0, 7.0)
        local tmp = converted_copy - converted_data
        tmp:set_mask(converted_data)
        converted_data = tmp
        aoflagger.set_progress(
          (ipol - 1) * iteration_count + i,
          #flag_polarizations * iteration_count
        )
      end
      aoflagger.sumthreshold(
        converted_data,
        base_threshold,
        base_threshold * transient_threshold_factor,
        true,
        true
      )
    end
    aoflagger.set_progress(ipol, #flag_polarizations)
  end
  aoflagger.scale_invariant_rank_operator(
    input,  0.2, 0.2
  )
  aoflagger.threshold_timestep_rms(input, 3.0)
  if input:is_complex() and input:has_metadata() then
    aoflagger.collect_statistics(input, copy_of_input)
  end
  input:flag_nans()
end"""


default_no_low_pass = """aoflagger.require_min_version("3.0")
function execute(input)
  local flag_polarizations = input:get_polarizations()
  local base_threshold = 2.0
  local flag_representations = { "amplitude" }
  local iteration_count = 3
  local threshold_factor_step = 4.0
  local transient_threshold_factor = 5.0
  input:clear_mask()
  local copy_of_input = input:copy()
  for ipol, polarization in ipairs(flag_polarizations) do
    local pol_data = input:convert_to_polarization(polarization)
    local converted_data
    local converted_copy
    for _, representation in ipairs(flag_representations) do
      converted_data = pol_data:convert_to_complex(representation)
      converted_copy = converted_data:copy()
      for i = 1, iteration_count - 1 do
        local threshold_factor = threshold_factor_step ^ (iteration_count - i)
        local sumthr_level = threshold_factor * base_threshold
        aoflagger.sumthreshold(
          converted_data,
          sumthr_level,
          sumthr_level * transient_threshold_factor,
          true,
          true
        )
        local chdata = converted_data:copy()
        aoflagger.threshold_timestep_rms(converted_data, 3.0)
        aoflagger.threshold_channel_rms(chdata, 3.0 * threshold_factor, true)
        converted_data:join_mask(chdata)
        converted_data:set_visibilities(converted_copy)
        local tmp = converted_copy - converted_data
        tmp:set_mask(converted_data)
        converted_data = tmp
        aoflagger.set_progress(
          (ipol - 1) * iteration_count + i,
          #flag_polarizations * iteration_count
        )
      end
      aoflagger.sumthreshold(
        converted_data,
        base_threshold,
        base_threshold * transient_threshold_factor,
        true,
        true
      )
    end
    aoflagger.set_progress(ipol, #flag_polarizations)
  end
  aoflagger.scale_invariant_rank_operator(
    input,  0.2, 0.2
  )
  aoflagger.threshold_timestep_rms(input, 3.0)
  if input:is_complex() and input:has_metadata() then
    aoflagger.collect_statistics(input, copy_of_input)
  end
  input:flag_nans()
end"""
