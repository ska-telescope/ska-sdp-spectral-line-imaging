aoflagger.require_min_version("3.0")
function execute(input)

   function contains(arr, val)
      for _, v in ipairs(arr) do
         if v == val then
            return true
         end
      end
      return false
   end

   local flag_polarizations = input:get_polarizations()

   local base_threshold = {base_threshold}
   local flag_representations = { "amplitude" }
   local iteration_count = {iteration_count}
   local threshold_factor_step = {threshold_factor_step}
   local keep_original_flags = {keep_original_flags}
   local transient_threshold_factor = {transient_threshold_factor}
   local threshold_timestep_rms = {threshold_timestep_rms}
   local threshold_channel_rms = {threshold_channel_rms}
   local keep_outliers = {keep_outliers}
   local do_low_pass = {do_low_pass}

   local inpPolarizations = input:get_polarizations()

   if not keep_original_flags then
      input:clear_mask()
   end

   local copy_of_input = input:copy()

   for ipol, polarization in ipairs(flag_polarizations) do
      local pol_data = input:convert_to_polarization(polarization)
      local converted_data
      local converted_copy

      for _, representation in ipairs(flag_representations) do
         converted_data = pol_data:convert_to_complex(representation)
         converted_copy = converted_data:copy()

         for i = 1, iteration_count - 1 do
            local threshold_factor = threshold_factor_step ^ (
               iteration_count - i
            )
            local sumthr_level = threshold_factor * base_threshold

            if keep_original_flags then
               aoflagger.sumthreshold_masked(
                  converted_data,
                  converted_copy,
                  sumthr_level,
                  sumthr_level * transient_threshold_factor,
                  true,
                  true
               )
            else
               aoflagger.sumthreshold(
                  converted_data,
                  sumthr_level,
                  sumthr_level * transient_threshold_factor,
                  true,
                  true
               )
            end

            local chdata = converted_data:copy()
            aoflagger.threshold_timestep_rms(
               converted_data,
               threshold_timestep_rms
            )

            aoflagger.threshold_channel_rms(
               chdata,
               threshold_channel_rms * threshold_factor,
               keep_outliers
            )

            converted_data:join_mask(chdata)
            converted_data:set_visibilities(converted_copy)
            if keep_original_flags then
               converted_data:join_mask(converted_copy)
            end

            if do_low_pass then
               aoflagger.low_pass_filter(
                  converted_data,
                  {window_size_str},
                  {time_sigma},
                  {freq_sigma}
               )
            end

            local tmp = converted_copy - converted_data
            tmp:set_mask(converted_data)
            converted_data = tmp
            aoflagger.set_progress(
               (ipol - 1) * iteration_count + i,
               #flag_polarizations * iteration_count
            )
         end

         if keep_original_flags then
            aoflagger.sumthreshold_masked(
               converted_data,
               converted_copy,
               base_threshold,
               base_threshold * transient_threshold_factor,
               true,
               true
            )
         else
            aoflagger.sumthreshold(
               converted_data,
               base_threshold,
               base_threshold * transient_threshold_factor,
               true,
               true
            )
         end
      end

      if keep_original_flags then
         converted_data:join_mask(converted_copy)
      end

      if contains(inpPolarizations, polarization) then
         if input:is_complex() then
            converted_data = converted_data:convert_to_complex("complex")
         end
         input:set_polarization_data(polarization, converted_data)
      else
         input:join_mask(converted_data)
      end

      aoflagger.set_progress(ipol, #flag_polarizations)
   end

   if keep_original_flags then
      aoflagger.scale_invariant_rank_operator_masked(
         input,
         copy_of_input,
         0.2,
         0.2
      )
   else
      aoflagger.scale_invariant_rank_operator(input, 0.2, 0.2)
   end

   aoflagger.threshold_timestep_rms(input, threshold_timestep_rms)

   if input:is_complex() and input:has_metadata() then
      aoflagger.collect_statistics(input, copy_of_input)
   end
   input:flag_nans()
end
