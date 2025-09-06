pub fn find_by_name<'a>(
    data_planes: &'a [tables::DataPlane],
    data_plane_name: &str,
) -> Result<&'a tables::DataPlane, Option<&'a str>> {
    if let Some(data_plane) = data_planes
        .iter()
        .find(|dp| dp.data_plane_name == data_plane_name)
    {
        return Ok(data_plane);
    }

    let suggest = data_planes
        .iter()
        .map(|dp| {
            (
                strsim::osa_distance(data_plane_name, &dp.data_plane_name),
                &dp.data_plane_name,
            )
        })
        .min()
        .map(|(_, suggest_name)| suggest_name.as_str());

    Err(suggest)
}
