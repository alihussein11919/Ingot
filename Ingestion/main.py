from .producer import publish
from .collectors.latest import latest
from .collectors.spot import spot
from .collectors.currencies import currencies
from .collectors.authority import authority

publish("latest_prices", latest())
publish("spot_prices", spot())
publish("currency_rates", currencies())
publish("authority_prices", authority())