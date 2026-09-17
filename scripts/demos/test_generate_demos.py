import unittest

from generate_demos import _home_demo_env


class HomeDemoEnvironmentTest(unittest.TestCase):
    def test_home_tapes_remove_machine_specific_cloud_settings(self) -> None:
        source = {
            "PATH": "/usr/bin",
            "AWS_PROFILE": "personal",
            "AZURE_CLIENT_ID": "secret",
            "CLOUDSDK_CONFIG": "/private/gcloud",
            "GOOGLE_APPLICATION_CREDENTIALS": "/private/key.json",
            "MC_HOST_LAB": "https://key:secret@example.test",
            "ECS_CONTAINER_CREDENTIALS_RELATIVE_URI": "/credentials",
            "K_SERVICE": "private-service",
            "NO_COLOR": "1",
        }

        result = _home_demo_env(source, 14)

        self.assertEqual(result["PATH"], "/usr/bin")
        self.assertEqual(result["HOME"], "/tmp/datui-demo/home")
        self.assertEqual(result["COLORTERM"], "truecolor")
        self.assertEqual(result["DATUI_CACHE_DIR"], "/tmp/datui-demo/cache")
        self.assertEqual(result["XDG_CONFIG_HOME"], "/tmp/datui-demo/config")
        self.assertFalse((set(source) - {"PATH"}) & set(result))
        self.assertNotIn("HOME", source)

    def test_other_tapes_keep_their_environment(self) -> None:
        source = {"PATH": "/usr/bin", "AWS_PROFILE": "recording-profile"}

        self.assertIs(_home_demo_env(source, 11), source)


if __name__ == "__main__":
    unittest.main()
