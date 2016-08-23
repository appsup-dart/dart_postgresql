

import 'package:test/test.dart';
import 'package:postgresql/postgresql.dart';
import 'dart:async';
import 'package:yaml/yaml.dart';
import 'dart:io';

/**
 * Loads configuration from yaml file into [Settings].
 */
Settings loadSettings(){
  var map = loadYaml(new File('test/test_config.yaml').readAsStringSync());
  return new Settings.fromMap(map);
}
main() {
  String validUri = loadSettings().toUri();

  test("Copy", () async {
    var conn = await connect(validUri);
    await conn.execute('create temporary table dart_unit_test (a timestamp, b integer, c text, d jsonb, e boolean)');

    var data = [];
    data.add([new DateTime.now(), 12, "hello world", {"x": 4}, true]);
    data.add([new DateTime.now(), null, "\\N", [1, 2], false]);
    data.add([new DateTime.now(), null, "\N", [1, 2], null]);
    data.add([new DateTime.now(), 12, "he\bllo\n wo\rrl\td \N \\N and \veve\fry ", {"x": 4}, false]);

    var controller = new StreamController();

    var f = conn.copyIn("dart_unit_test", controller.stream);

    data.forEach((r)=>controller.add(r));

    controller.close();

    expect(await f, 4);

    var out = await conn.copyOut("dart_unit_test", columns: {
      "a": DateTime,
      "b": int,
      "c": String,
      "d": Object,
      "e": bool
    }).toList();

    expect(out.map((r)=>r.toList()), data);

  });

}