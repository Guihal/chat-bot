import { checkChannelRights } from "./extensions/lib/botapi.ts";

try {
	const result = await checkChannelRights();
	console.log(JSON.stringify(result));
} catch (error) {
	console.error(`CHECK_CHANNEL_FAIL ${error.message}`);
	process.exit(1);
}
