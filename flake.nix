{
  description = "jaccard-store";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

    systems.url = "github:nix-systems/default";

    gonixgo = {
      url = "github:draganm/gonixgo/v0.2.0";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.systems.follows = "systems";
    };

  };

  outputs = { self, nixpkgs, systems, gonixgo, ... }@inputs:
    let
      eachSystem = f:
        nixpkgs.lib.genAttrs (import systems)
        (system: f system nixpkgs.legacyPackages.${system});
    in {

      # Evaluating a gonixgo package runs `gonixgo resolve` through
      # builtins.exec, so every command that touches `packages` needs
      #   --option allow-unsafe-native-code-during-evaluation true
      packages = eachSystem (system: pkgs:
        let
          # This pkgs builds gonixgo's own tool and the Go programs.
          goEnv = gonixgo.lib.mkGoEnv { inherit pkgs; };
        in {
          # Both commands: the server, jaccard-stored, and the client,
          # jaccard-store.
          default = goEnv.buildGoApplication {
            pname = "jaccard-store";
            src = ./.;
            subPackages = [ "cmd/jaccard-stored" "cmd/jaccard-store" ];
            meta.license = pkgs.lib.licenses.lgpl3Only;
          };
        });

      # The shell carries the built commands, so entering it (or direnv
      # reloading it after a source change, see .envrc) rebuilds them. It
      # therefore needs the same evaluation option as `packages`.
      devShells = eachSystem (system: pkgs: {
        default = pkgs.mkShell {
          shellHook = ''
            # Set here the env vars you want to be available in the shell
          '';
          hardeningDisable = [ "all" ];

          packages = with pkgs; [ go sqlc self.packages.${system}.default ];
        };

        # The same tools without the built commands. The default shell
        # cannot be entered while the module does not resolve, for example
        # after a `go get` that left go.sum behind; this one can, to run
        # `go mod tidy`. It needs no evaluation option.
        bare = pkgs.mkShell {
          hardeningDisable = [ "all" ];

          packages = with pkgs; [ go sqlc ];
        };
      });
    };
}
