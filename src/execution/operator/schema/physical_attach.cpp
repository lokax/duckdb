#include "duckdb/execution/operator/schema/physical_attach.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class AttachSourceState : public GlobalSourceState {
public:
	AttachSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalAttach::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<AttachSourceState>();
}

void PhysicalAttach::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                             LocalSourceState &lstate) const {
	auto &state = gstate.Cast<AttachSourceState>();
	if (state.finished) {
		return;
	}
	// parse the options
	auto &config = DBConfig::GetConfig(context.client);
	AccessMode access_mode = config.options.access_mode;
	string type;
	string unrecognized_option;
	for (auto &entry : info->options) {
		if (entry.first == "readonly" || entry.first == "read_only") {
			auto read_only = BooleanValue::Get(entry.second.DefaultCastAs(LogicalType::BOOLEAN));
			if (read_only) {
				access_mode = AccessMode::READ_ONLY;
			} else {
				access_mode = AccessMode::READ_WRITE;
			}
		} else if (entry.first == "readwrite" || entry.first == "read_write") {
			auto read_only = !BooleanValue::Get(entry.second.DefaultCastAs(LogicalType::BOOLEAN));
			if (read_only) {
				access_mode = AccessMode::READ_ONLY;
			} else {
				access_mode = AccessMode::READ_WRITE;
			}
		} else if (entry.first == "type") {
			type = StringValue::Get(entry.second.DefaultCastAs(LogicalType::VARCHAR));
		} else if (unrecognized_option.empty()) {
			unrecognized_option = entry.first;
		}
	}
	auto &db = DatabaseInstance::GetDatabase(context.client);
	if (type.empty()) {
		// try to extract type from path
		type = db.ExtractDatabaseType(info->path);
	}
	if (!type.empty()) {
		type = ExtensionHelper::ApplyExtensionAlias(type);
	}
	if (type.empty() && !unrecognized_option.empty()) {
		throw BinderException("Unrecognized option for attach \"%s\"", unrecognized_option);
	}

	// if we are loading a database type from an extension - check if that extension is loaded
	if (!type.empty()) {
		if (!db.ExtensionIsLoaded(type)) {
			ExtensionHelper::LoadExternalExtension(context.client, type);
		}
	}

	// attach the database
	auto &name = info->name;
	const auto &path = info->path;

	if (name.empty()) {
		name = AttachedDatabase::ExtractDatabaseName(path);
	}
	auto &db_manager = DatabaseManager::Get(context.client);
	auto existing_db = db_manager.GetDatabaseFromPath(context.client, path);
	if (existing_db) {
		throw BinderException("Database \"%s\" is already attached with alias \"%s\"", path, existing_db->GetName());
	}
	auto new_db = db.CreateAttachedDatabase(*info, type, access_mode);
	new_db->Initialize();

	db_manager.AddDatabase(context.client, std::move(new_db));
	state.finished = true;
}

} // namespace duckdb
