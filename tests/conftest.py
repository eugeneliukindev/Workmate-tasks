from contextlib import AbstractContextManager

from _pytest.raises import RaisesExc

NO_RESULT = ...
type ExpectationType = AbstractContextManager[None | RaisesExc[Exception]]
