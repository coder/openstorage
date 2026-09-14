variable "region" {
  description = "AWS commercial region with at least two available AZs."
  type        = string
  validation {
    condition     = can(regex("^[a-z]{2}-[a-z]+-[0-9]+$", var.region))
    error_message = "Use an AWS region such as us-east-1."
  }
}

variable "name" {
  description = "Unique deployment name in this account/region; used for resource names and tags."
  type        = string
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,19}$", var.name)) && !endswith(var.name, "-")
    error_message = "Use 3–20 lowercase letters, digits or hyphens, starting with a letter and not ending in a hyphen."
  }
}

variable "org" {
  description = "JWT issuer / organization name."
  type        = string
  validation {
    condition     = can(regex("^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$", var.org))
    error_message = "Use 1–63 letters, digits, underscores or hyphens, starting with a letter or digit."
  }
}

variable "org_public_key_pem" {
  description = "PKIX PUBLIC KEY PEM only. The JWT signing private key must never enter Terraform."
  type        = string
  validation {
    condition     = can(regex("^-----BEGIN PUBLIC KEY-----\\n[A-Za-z0-9+/=\\n\\r]+\\n-----END PUBLIC KEY-----$", trimspace(var.org_public_key_pem)))
    error_message = "Pass a single PKIX PUBLIC KEY PEM, never a private key."
  }
}

variable "release_tag" {
  description = "Exact release or RC tag embedded in the artifact (for example v0.15.0)."
  type        = string
  validation {
    condition     = can(regex("^v[0-9]+\\.[0-9]+\\.[0-9]+(-[A-Za-z0-9.-]+)?$", var.release_tag))
    error_message = "An exact vX.Y.Z release or prerelease tag is required; latest is forbidden."
  }
}

variable "release_archive" {
  description = "Local path to the verified linux_amd64 release archive. Terraform uploads it privately; no GitHub token reaches EC2."
  type        = string
}

variable "release_sha256" {
  description = "SHA-256 of the archive, taken from the pinned release checksums.txt (not a guessed or substituted digest)."
  type        = string
  validation {
    condition     = can(regex("^[a-f0-9]{64}$", var.release_sha256))
    error_message = "Provide a lowercase 64-character SHA-256 digest."
  }
}

variable "allowed_client_cidrs" {
  description = "IPv4 networks allowed to reach HTTPS. Include the user or same-region smoke runner's egress address."
  type        = set(string)
  validation {
    condition = length(var.allowed_client_cidrs) > 0 && alltrue([
      for cidr in var.allowed_client_cidrs : try(cidrnetmask(cidr) != "0.0.0.0", false)
    ])
    error_message = "Provide at least one IPv4 CIDR; unrestricted 0.0.0.0/0 is not supported by this evaluation reference."
  }
}

variable "allow_destroy_persistent_data" {
  description = "DANGER: permit permanent deletion of the database and all bucket data. Apply this change before destroy."
  type        = bool
  default     = false
}

variable "ami_id" {
  description = "Optional previously recorded AL2023 x86_64 AMI ID for replay; otherwise resolve AWS's current AL2023 AMI."
  type        = string
  default     = null
  validation {
    condition     = var.ami_id == null ? true : can(regex("^ami-[a-f0-9]{8,17}$", var.ami_id))
    error_message = "Use an AL2023 x86_64 AMI ID."
  }
}
