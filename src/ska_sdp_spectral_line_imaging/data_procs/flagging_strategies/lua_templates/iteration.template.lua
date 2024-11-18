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
        {sumthreshold_sumthr_masked}
        local chdata = converted_data:copy()
        aoflagger.threshold_timestep_rms(converted_data, {threshold_timestep_rms})
        aoflagger.threshold_channel_rms(chdata, {threshold_channel_rms} * threshold_factor, {outlier_text})
        converted_data:join_mask(chdata)
        converted_data:set_visibilities(converted_copy)
        {converted_join_copy_mask}
        {low_pass_filter_template}
        local tmp = converted_copy - converted_data
        tmp:set_mask(converted_data)
        converted_data = tmp
        aoflagger.set_progress(
          (ipol - 1) * iteration_count + i,
          #flag_polarizations * iteration_count
        )
      end
      {sumthreshold_base_masked}
    end
    {converted_join_copy_mask}
    aoflagger.set_progress(ipol, #flag_polarizations)
  end