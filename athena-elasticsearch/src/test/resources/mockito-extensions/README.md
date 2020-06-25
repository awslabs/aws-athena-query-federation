# Mockito Configuration

The file `org.mockito.plugins.MockMaker` contains a configuration (**mock-maker-inline**) allowing Mockito to mock
methods and objects declared `final`.

**NOTE: Failure to include this configuration will result in a runtime error when attempting to mock final methods
and objects:**

```
Mockito cannot mock/spy because :
– final class
```