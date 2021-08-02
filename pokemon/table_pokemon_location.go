package pokemon

import (
	"context"

	"github.com/mtslzr/pokeapi-go"
	"github.com/mtslzr/pokeapi-go/structs"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/plugin"
	"github.com/turbot/steampipe-plugin-sdk/plugin/transform"
)

func tablePokemonLocation(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name: "pokemon_location",
		Description:  "Locations that can be visited within the games. Locations make up sizable portions of regions, like cities or routes.",
		List: &plugin.ListConfig{
			Hydrate: listLocations,
		},
		Get: &plugin.GetConfig{
			KeyColumns: plugin.AnyColumn([]string{"name"}),
			Hydrate: getLocation,
			ShouldIgnoreError: isNotFoundError([]string{"invalid character 'N' looking for beginning of value"}),			
		},
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Description: "The name for this resource.",
				Type:        proto.ColumnType_STRING,				
			},
			{
				Name:        "areas",
				Description: "Areas that can be found in this location",
				Type:        proto.ColumnType_JSON,
				Hydrate:     getLocation,
			},
			{
				Name:        "game_indices",
				Description: "A list of game indices relevant to location item by generation",
				Type:        proto.ColumnType_JSON,
				Hydrate:     getLocation,
			},
			{
				Name:        "id",
				Description: "The identifier for this resource.",
				Type:        proto.ColumnType_INT,
				Hydrate:     getLocation,
				Transform:   transform.FromGo(),
			},

			{
				Name:        "names",
				Description: "Name of the region in different languages",
				Type:        proto.ColumnType_JSON,
				Hydrate:     getLocation,
			},
			// Standard columns
			{
				Name: 			"title",
				Description: 	"Title of the resource.",
				Type: 			proto.ColumnType_STRING,
				Transform: 		transform.FromField("Name"),
			},
		},
	}
}

func listLocations(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)
	logger.Trace("listLocations")

	offset := 0

	for true {
		resources, err := pokeapi.Resource("location", offset)

		if err != nil {
			plugin.Logger(ctx).Error("pokemon_location.listLocations", "query_error", err)
			return nil, err
		}

		for _, pokemon := range resources.Results {
			d.StreamListItem(ctx, pokemon)
		}

		// No next URL returned
		if len(resources.Next) == 0 {
			break
		}

		urlOffset, err := extractUrlOffset(resources.Next)
		if err != nil {
			plugin.Logger(ctx).Error("pokemon_location.listLocations", "extract_url_offset_error", err)
			return nil, err
		}

		// Set next offset
		offset = urlOffset
	}
	return nil, nil
}

func getLocation(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)
	logger.Trace("getLocation")

	var name string

	if h.Item != nil {
		result := h.Item.(structs.Result)
		name = result.Name
	} else {
		name = d.KeyColumnQuals["name"].GetStringValue()
	}

	logger.Debug("Name", name)

	location, err := pokeapi.Location(name)

	if err != nil {
		plugin.Logger(ctx).Error("pokemon_location.getLocation", "query_error", err)
		return nil, err
	}

	return location, nil
}