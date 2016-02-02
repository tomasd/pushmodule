package com.drencak.push.server;

import javaslang.Function1;

public interface StateProvider extends Function1<String, Iterable<Message>> {

}
